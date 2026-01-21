package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Config struct {
	NATSUrl      string
	Rate         int
	Duration     time.Duration
	Chains       []string
	BurstMode    bool
	BurstRatio   float64
	BurstPeriod  time.Duration
	StreamName   string
	SubjectBase  string
	WorkerCount  int
}

type CanonicalEvent struct {
	EventID         string    `json:"event_id"`
	Chain           int       `json:"chain"`
	CommitmentLevel int       `json:"commitment_level"`
	BlockNumber     uint64    `json:"block_number"`
	BlockHash       string    `json:"block_hash"`
	TxHash          string    `json:"tx_hash"`
	EventType       string    `json:"event_type"`
	Accounts        []string  `json:"accounts"`
	Timestamp       time.Time `json:"timestamp"`
	Payload         []byte    `json:"payload,omitempty"`
	ReorgAction     int       `json:"reorg_action"`
	ProgramID       string    `json:"program_id,omitempty"`
	NativeValue     uint64    `json:"native_value"`
	PublishedAt     time.Time `json:"published_at"`
}

var chainValues = map[string]int{
	"ethereum": 1,
	"solana":   2,
	"polygon":  3,
	"arbitrum": 4,
	"optimism": 5,
	"base":     6,
	"bsc":      7,
	"avalanche": 8,
}

var eventTypes = []string{
	"transfer",
	"swap",
	"mint",
	"burn",
	"stake",
	"unstake",
	"approve",
	"deposit",
	"withdraw",
	"liquidation",
}

type Metrics struct {
	Published    atomic.Int64
	Errors       atomic.Int64
	BytesSent    atomic.Int64
	AvgLatencyNs atomic.Int64
}

func main() {
	cfg := parseFlags()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("shutdown signal received")
		cancel()
	}()

	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("generator failed", "error", err)
		os.Exit(1)
	}
}

func parseFlags() Config {
	cfg := Config{}

	flag.StringVar(&cfg.NATSUrl, "nats", "nats://localhost:4222", "NATS server URL")
	flag.IntVar(&cfg.Rate, "rate", 1000, "Events per second")
	flag.DurationVar(&cfg.Duration, "duration", time.Minute, "Test duration")
	chainsStr := flag.String("chains", "ethereum,solana", "Comma-separated list of chains")
	flag.BoolVar(&cfg.BurstMode, "burst", false, "Enable burst mode")
	flag.Float64Var(&cfg.BurstRatio, "burst-ratio", 10.0, "Burst rate multiplier")
	flag.DurationVar(&cfg.BurstPeriod, "burst-period", 30*time.Second, "Time between bursts")
	flag.StringVar(&cfg.StreamName, "stream", "CANONICAL_EVENTS", "JetStream stream name")
	flag.StringVar(&cfg.SubjectBase, "subject", "events.canonical", "Subject prefix")
	flag.IntVar(&cfg.WorkerCount, "workers", 4, "Number of publisher workers")

	flag.Parse()

	cfg.Chains = strings.Split(*chainsStr, ",")
	for i, c := range cfg.Chains {
		cfg.Chains[i] = strings.TrimSpace(c)
	}

	return cfg
}

func run(ctx context.Context, cfg Config, logger *slog.Logger) error {
	logger.Info("starting event generator",
		"rate", cfg.Rate,
		"duration", cfg.Duration,
		"chains", cfg.Chains,
		"burst_mode", cfg.BurstMode,
		"nats_url", cfg.NATSUrl,
	)

	nc, err := nats.Connect(cfg.NATSUrl,
		nats.Name("pulse-load-generator"),
		nats.ReconnectWait(time.Second),
		nats.MaxReconnects(10),
	)
	if err != nil {
		return fmt.Errorf("NATS connect failed: %w", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("JetStream context failed: %w", err)
	}

	stream, err := js.Stream(ctx, cfg.StreamName)
	if err != nil {
		streamCfg := jetstream.StreamConfig{
			Name:        cfg.StreamName,
			Subjects:    []string{cfg.SubjectBase + ".>"},
			Retention:   jetstream.LimitsPolicy,
			MaxAge:      24 * time.Hour,
			MaxBytes:    10 * 1024 * 1024 * 1024,
			Compression: jetstream.S2Compression,
			Replicas:    1,
		}
		stream, err = js.CreateOrUpdateStream(ctx, streamCfg)
		if err != nil {
			return fmt.Errorf("stream create/update failed: %w", err)
		}
		logger.Info("stream created/updated", "name", cfg.StreamName)
	}

	info, _ := stream.Info(ctx)
	logger.Info("connected to stream",
		"stream", info.Config.Name,
		"subjects", info.Config.Subjects,
	)

	metrics := &Metrics{}
	go reportMetrics(ctx, metrics, logger)

	eventCh := make(chan *CanonicalEvent, cfg.Rate*2)

	for i := 0; i < cfg.WorkerCount; i++ {
		go publishWorker(ctx, js, cfg, eventCh, metrics, logger, i)
	}

	err = generateEvents(ctx, cfg, eventCh, metrics, logger)

	close(eventCh)
	time.Sleep(time.Second)

	logger.Info("generation complete",
		"published", metrics.Published.Load(),
		"errors", metrics.Errors.Load(),
		"bytes_sent", metrics.BytesSent.Load(),
	)

	return err
}

func generateEvents(ctx context.Context, cfg Config, eventCh chan<- *CanonicalEvent, metrics *Metrics, logger *slog.Logger) error {
	endTime := time.Now().Add(cfg.Duration)
	ticker := time.NewTicker(time.Second / time.Duration(cfg.Rate))
	defer ticker.Stop()

	burstTicker := time.NewTicker(cfg.BurstPeriod)
	defer burstTicker.Stop()

	inBurst := false
	burstEndTime := time.Time{}

	var blockNums = make(map[string]uint64)
	for _, chain := range cfg.Chains {
		blockNums[chain] = 18000000 + uint64(rand.Intn(1000000))
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-burstTicker.C:
			if cfg.BurstMode && !inBurst {
				inBurst = true
				burstEndTime = time.Now().Add(5 * time.Second)
				logger.Info("burst started", "ratio", cfg.BurstRatio)
			}

		case <-ticker.C:
			if time.Now().After(endTime) {
				return nil
			}

			if inBurst && time.Now().After(burstEndTime) {
				inBurst = false
				logger.Info("burst ended")
			}

			count := 1
			if inBurst {
				count = int(cfg.BurstRatio)
			}

			for i := 0; i < count; i++ {
				chain := cfg.Chains[rand.Intn(len(cfg.Chains))]
				blockNums[chain]++

				event := generateEvent(chain, blockNums[chain])

				select {
				case eventCh <- event:
				default:
					metrics.Errors.Add(1)
				}
			}
		}
	}
}

func generateEvent(chain string, blockNum uint64) *CanonicalEvent {
	now := time.Now()

	numAccounts := 2 + rand.Intn(4)
	accounts := make([]string, numAccounts)
	for i := range accounts {
		accounts[i] = randomHex(40)
	}

	return &CanonicalEvent{
		EventID:         fmt.Sprintf("evt_%d_%s", now.UnixNano(), randomHex(8)),
		Chain:           chainValues[chain],
		CommitmentLevel: 3,
		BlockNumber:     blockNum,
		BlockHash:       randomHex(64),
		TxHash:          randomHex(64),
		EventType:       eventTypes[rand.Intn(len(eventTypes))],
		Accounts:        accounts,
		Timestamp:       now,
		NativeValue:     uint64(rand.Int63n(1e18)),
		PublishedAt:     now,
	}
}

func publishWorker(ctx context.Context, js jetstream.JetStream, cfg Config, eventCh <-chan *CanonicalEvent, metrics *Metrics, logger *slog.Logger, workerID int) {
	for event := range eventCh {
		select {
		case <-ctx.Done():
			return
		default:
		}

		data, err := json.Marshal(event)
		if err != nil {
			metrics.Errors.Add(1)
			continue
		}

		subject := fmt.Sprintf("%s.%d.%s", cfg.SubjectBase, event.Chain, event.EventType)
		start := time.Now()

		_, err = js.Publish(ctx, subject, data)
		if err != nil {
			metrics.Errors.Add(1)
			if ctx.Err() == nil {
				logger.Warn("publish failed", "worker", workerID, "error", err)
			}
			continue
		}

		latency := time.Since(start).Nanoseconds()
		metrics.Published.Add(1)
		metrics.BytesSent.Add(int64(len(data)))

		current := metrics.AvgLatencyNs.Load()
		newAvg := (current*9 + latency) / 10
		metrics.AvgLatencyNs.Store(newAvg)
	}
}

func reportMetrics(ctx context.Context, metrics *Metrics, logger *slog.Logger) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastPublished int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			published := metrics.Published.Load()
			rate := (published - lastPublished) / 5
			lastPublished = published

			logger.Info("metrics",
				"published", published,
				"rate_per_sec", rate,
				"errors", metrics.Errors.Load(),
				"bytes_sent_mb", metrics.BytesSent.Load()/(1024*1024),
				"avg_latency_ms", float64(metrics.AvgLatencyNs.Load())/1e6,
			)
		}
	}
}

func randomHex(length int) string {
	const chars = "0123456789abcdef"
	b := make([]byte, length)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return "0x" + string(b)
}
