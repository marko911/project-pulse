// Package main implements the Gap Detector service.
// This service monitors finalized topics on Redpanda and detects missing blocks.
// If a gap is detected, it emits alerts and halts processing (fail-closed design).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	// Configuration flags
	var (
		brokers       = flag.String("brokers", envOrDefault("KAFKA_BROKERS", "localhost:9092"), "Redpanda/Kafka brokers (comma-separated)")
		topics        = flag.String("topics", envOrDefault("TOPICS", "finalized-events"), "Topics to monitor (comma-separated)")
		consumerGroup = flag.String("group", envOrDefault("CONSUMER_GROUP", "gap-detector"), "Consumer group ID")
		pollInterval  = flag.Duration("poll-interval", 100*time.Millisecond, "Polling interval")
		stateDir      = flag.String("state-dir", envOrDefault("STATE_DIR", "/var/lib/gap-detector"), "Directory for persistent state")
		alertWebhook  = flag.String("alert-webhook", envOrDefault("ALERT_WEBHOOK", ""), "Webhook URL for gap alerts")
		metricsAddr   = flag.String("metrics-addr", envOrDefault("METRICS_ADDR", ":9090"), "Address for metrics endpoint")
		maxGapBlocks  = flag.Int64("max-gap-blocks", envOrDefaultInt64("MAX_GAP_BLOCKS", 0), "Maximum allowed gap before alert (0 = any gap)")
		logLevel      = flag.String("log-level", envOrDefault("LOG_LEVEL", "info"), "Log level: debug, info, warn, error")
	)
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

	slog.Info("starting gap-detector",
		"brokers", *brokers,
		"topics", *topics,
		"consumer_group", *consumerGroup,
		"state_dir", *stateDir,
		"metrics_addr", *metricsAddr,
		"max_gap_blocks", *maxGapBlocks,
	)

	// Parse topics
	topicList := strings.Split(*topics, ",")
	for i := range topicList {
		topicList[i] = strings.TrimSpace(topicList[i])
	}

	// Parse brokers
	brokerList := strings.Split(*brokers, ",")
	for i := range brokerList {
		brokerList[i] = strings.TrimSpace(brokerList[i])
	}

	// Create detector configuration
	cfg := DetectorConfig{
		Brokers:       brokerList,
		Topics:        topicList,
		ConsumerGroup: *consumerGroup,
		PollInterval:  *pollInterval,
		StateDir:      *stateDir,
		AlertWebhook:  *alertWebhook,
		MetricsAddr:   *metricsAddr,
		MaxGapBlocks:  *maxGapBlocks,
	}

	// Create and initialize detector
	detector, err := NewDetector(cfg, logger)
	if err != nil {
		slog.Error("failed to create detector", "error", err)
		os.Exit(1)
	}

	// Setup context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		slog.Info("received shutdown signal", "signal", sig)
		cancel()
	}()

	// Run the detector (blocks until context is cancelled or fatal error)
	if err := detector.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("detector fatal error", "error", err)
		os.Exit(1)
	}

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := detector.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "error", err)
	}

	slog.Info("gap-detector shutdown complete")
}

func envOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func envOrDefaultInt64(key string, defaultVal int64) int64 {
	if val := os.Getenv(key); val != "" {
		var result int64
		if _, err := fmt.Sscanf(val, "%d", &result); err == nil {
			return result
		}
	}
	return defaultVal
}
