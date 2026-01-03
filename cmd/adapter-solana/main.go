// Command adapter-solana runs the Solana blockchain adapter service.
//
// This adapter connects to Solana via Yellowstone/Geyser gRPC and streams
// blockchain events (transactions, account updates) to the message broker.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mirador/pulse/internal/adapter"
	"github.com/mirador/pulse/internal/adapter/solana"
)

func main() {
	// Configuration flags
	geyserEndpoint := flag.String("geyser-endpoint", getEnv("GEYSER_ENDPOINT", "http://localhost:10000"), "Yellowstone/Geyser gRPC endpoint")
	geyserToken := flag.String("geyser-token", getEnv("GEYSER_TOKEN", ""), "Geyser authentication token")
	commitment := flag.String("commitment", getEnv("COMMITMENT_LEVEL", "confirmed"), "Commitment level: processed, confirmed, finalized")
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

	logger.Info("starting solana adapter",
		"geyser_endpoint", *geyserEndpoint,
		"commitment", *commitment,
	)

	// Create adapter configuration
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

	// Create adapter
	adpt := solana.New(cfg, logger)

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

	// Start event consumer (placeholder - will forward to broker)
	go func() {
		for event := range events {
			logger.Debug("received event",
				"block", event.BlockNumber,
				"tx", event.TxHash,
				"type", event.EventType,
			)
			// TODO: Forward to message broker (Redpanda/Kafka)
		}
	}()

	// Start adapter
	if err := adpt.Start(ctx, events); err != nil {
		logger.Error("failed to start adapter", "error", err)
		os.Exit(1)
	}

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
