// Command adapter-solana runs the Solana blockchain adapter service.
//
// This adapter connects to Solana via Yellowstone/Geyser gRPC (default) or
// standard WebSockets (legacy/free tier).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mirador/pulse/internal/adapter"
	"github.com/mirador/pulse/internal/adapter/solana"
	"github.com/mirador/pulse/internal/adapter/solana_ws"
)

// AdapterInterface defines the common interface for Solana adapters
type AdapterInterface interface {
	Start(ctx context.Context, events chan<- adapter.Event) error
	Stop(ctx context.Context) error
}

func main() {
	// Configuration flags
	adapterType := flag.String("type", getEnv("SOLANA_ADAPTER_TYPE", "grpc"), "Adapter type: grpc (Geyser) or ws (WebSocket)")
	geyserEndpoint := flag.String("geyser-endpoint", getEnv("GEYSER_ENDPOINT", "http://localhost:10000"), "Yellowstone/Geyser gRPC endpoint")
	geyserToken := flag.String("geyser-token", getEnv("GEYSER_TOKEN", ""), "Geyser authentication token")
	wsEndpoint := flag.String("ws-endpoint", getEnv("SOLANA_WS_ENDPOINT", "wss://api.devnet.solana.com"), "Solana WebSocket endpoint")
	commitment := flag.String("commitment", getEnv("COMMITMENT_LEVEL", "confirmed"), "Commitment level: processed, confirmed, finalized")
	brokerEndpoint := flag.String("broker", getEnv("BROKER_ENDPOINT", "localhost:9092"), "Redpanda/Kafka broker endpoint")
	outputTopic := flag.String("output-topic", getEnv("OUTPUT_TOPIC", "canonical-events"), "Topic to publish events to")
	logLevel := flag.String("log-level", getEnv("LOG_LEVEL", "info"), "Log level: debug, info, warn, error")
	flag.Parse()

	// Setup structured logging
	var level slog.Level
	switch *logLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))
	slog.SetDefault(logger)

	var adpt AdapterInterface

	// Initialize the requested adapter type
	if *adapterType == "ws" {
		logger.Info("initializing solana websocket adapter",
			"endpoint", *wsEndpoint,
			"commitment", *commitment,
		)
		cfg := solana_ws.Config{
			Config: adapter.Config{
				CommitmentLevel: *commitment,
				MaxRetries:      5,
				RetryDelayMs:    1000,
			},
			Endpoint: *wsEndpoint,
		}
		adpt = solana_ws.New(cfg, logger)
	} else {
		logger.Info("initializing solana geyser (gRPC) adapter",
			"endpoint", *geyserEndpoint,
			"commitment", *commitment,
		)
		cfg := solana.Config{
			Config: adapter.Config{
				CommitmentLevel: *commitment,
				MaxRetries:      5,
				RetryDelayMs:    1000,
			},
			GeyserEndpoint:        *geyserEndpoint,
			GeyserToken:           *geyserToken,
			SubscribeTransactions: true,
			SubscribeAccounts:     true,
		}
		adpt = solana.New(cfg, logger)
	}

	// Create event channel
	events := make(chan adapter.Event, 10000)

	// Setup context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Info("received shutdown signal", "signal", sig)
		cancel()
	}()

	// Initialize Kafka producer
	client, err := kgo.NewClient(
		kgo.SeedBrokers(*brokerEndpoint),
		kgo.DefaultProduceTopic(*outputTopic),
	)
	if err != nil {
		logger.Error("failed to create kafka client", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	// Start adapter
	if err := adpt.Start(ctx, events); err != nil {
		logger.Error("failed to start adapter", "error", err)
		os.Exit(1)
	}

	// Start event consumer
	go func() {
		for event := range events {
			// Convert to Canonical format (mocking processor for MVP)
			// In production, adapter sends raw events, processor normalizes.
			// Here we send JSON directly to trigger-router's input topic.
			canonical := map[string]interface{}{
				"event_id":         fmt.Sprintf("sol-%d-%s", event.BlockNumber, event.TxHash),
				"chain":            1, // SOLANA
				"commitment_level": 2, // CONFIRMED
				"block_number":     event.BlockNumber,
				"tx_hash":          event.TxHash,
				"event_type":       event.EventType,
				"timestamp":        time.Unix(event.Timestamp, 0).Format(time.RFC3339),
				"payload":          event.Payload, // Raw JSON
			}

			data, err := json.Marshal(canonical)
			if err != nil {
				logger.Error("marshal error", "error", err)
				continue
			}

			logger.Debug("publishing event",
				"block", event.BlockNumber,
				"tx", event.TxHash,
			)

			record := &kgo.Record{
				Value: data,
			}
			client.Produce(ctx, record, nil)
		}
	}()

	// Wait for shutdown
	<-ctx.Done()

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30)
	defer shutdownCancel()

	if err := adpt.Stop(shutdownCtx); err != nil {
		logger.Error("error during shutdown", "error", err)
	}

	close(events)
	logger.Info("solana adapter shutdown complete")
}

// getEnv returns environment variable value or default.
func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}