package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/marko911/project-pulse/internal/backfill"
)

func main() {
	var (
		brokers       = flag.String("brokers", envOrDefault("KAFKA_BROKERS", "localhost:9092"), "Kafka/Redpanda brokers (comma-separated)")
		gapTopic      = flag.String("gap-topic", envOrDefault("GAP_EVENTS_TOPIC", "gap-events"), "Topic to consume gap events from")
		backfillTopic = flag.String("backfill-topic", envOrDefault("BACKFILL_REQUESTS_TOPIC", "backfill-requests"), "Topic to publish backfill requests to")
		resultsTopic  = flag.String("results-topic", envOrDefault("BACKFILL_RESULTS_TOPIC", "backfill-results"), "Topic to consume backfill results from")
		consumerGroup = flag.String("group", envOrDefault("CONSUMER_GROUP", "backfill-orchestrator"), "Consumer group ID")
		maxRetries    = flag.Int("max-retries", envOrDefaultInt("MAX_RETRIES", 3), "Maximum retries per backfill request")
		metricsAddr   = flag.String("metrics-addr", envOrDefault("METRICS_ADDR", ":9091"), "Address for metrics endpoint")
		logLevel      = flag.String("log-level", envOrDefault("LOG_LEVEL", "info"), "Log level: debug, info, warn, error")
	)
	flag.Parse()

	logger := setupLogger(*logLevel)
	slog.SetDefault(logger)

	slog.Info("starting backfill-orchestrator",
		"brokers", *brokers,
		"gap_topic", *gapTopic,
		"backfill_topic", *backfillTopic,
		"results_topic", *resultsTopic,
		"consumer_group", *consumerGroup,
		"max_retries", *maxRetries,
		"metrics_addr", *metricsAddr,
	)

	brokerList := strings.Split(*brokers, ",")
	for i := range brokerList {
		brokerList[i] = strings.TrimSpace(brokerList[i])
	}

	cfg := backfill.OrchestratorConfig{
		Brokers:               brokerList,
		GapEventsTopic:        *gapTopic,
		BackfillRequestsTopic: *backfillTopic,
		BackfillResultsTopic:  *resultsTopic,
		ConsumerGroup:         *consumerGroup,
		MaxRetries:            *maxRetries,
		DefaultPriority:       backfill.BackfillPriorityNormal,
		MaxConcurrentBackfills: 10,
	}

	orchestrator, err := backfill.NewOrchestrator(cfg, logger)
	if err != nil {
		slog.Error("failed to create orchestrator", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		slog.Info("received shutdown signal", "signal", sig)
		cancel()
	}()

	if *metricsAddr != "" {
		go runMetricsServer(*metricsAddr, orchestrator, logger)
	}

	if err := orchestrator.Start(ctx); err != nil {
		slog.Error("failed to start orchestrator", "error", err)
		os.Exit(1)
	}

	slog.Info("backfill orchestrator running, consuming gap events...")

	if err := orchestrator.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("orchestrator error", "error", err)
		os.Exit(1)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := orchestrator.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "error", err)
	}

	stats := orchestrator.GetStats()
	slog.Info("backfill orchestrator shutdown",
		"gaps_received", stats.GapsReceived,
		"backfills_created", stats.BackfillsCreated,
		"backfills_complete", stats.BackfillsComplete,
		"backfills_failed", stats.BackfillsFailed,
		"total_blocks_filled", stats.TotalBlocksFilled,
	)
}

func runMetricsServer(addr string, orchestrator *backfill.Orchestrator, logger *slog.Logger) {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy"}`))
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		stats := orchestrator.GetStats()
		metrics := map[string]interface{}{
			"gaps_received":       stats.GapsReceived,
			"backfills_created":   stats.BackfillsCreated,
			"backfills_complete":  stats.BackfillsComplete,
			"backfills_failed":    stats.BackfillsFailed,
			"total_blocks_filled": stats.TotalBlocksFilled,
			"pending_count":       orchestrator.GetPendingCount(),
			"active_count":        orchestrator.GetActiveCount(),
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(metrics)
	})

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	logger.Info("starting metrics server", "addr", addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("metrics server error", "error", err)
	}
}

func setupLogger(levelStr string) *slog.Logger {
	var level slog.Level
	switch strings.ToLower(levelStr) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))
}

func envOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func envOrDefaultInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		var result int
		for _, c := range val {
			if c < '0' || c > '9' {
				return defaultVal
			}
			result = result*10 + int(c-'0')
		}
		return result
	}
	return defaultVal
}
