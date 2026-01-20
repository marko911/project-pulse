// Package main implements the outbox publisher service.
// This service polls the transactional outbox table and publishes events to Redpanda/Kafka.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/marko911/project-pulse/internal/platform/storage"
)

func main() {
	// Configuration flags
	var (
		dbHost     = flag.String("db-host", envOrDefault("DB_HOST", "localhost"), "Database host")
		dbPort     = flag.Int("db-port", envOrDefaultInt("DB_PORT", 5432), "Database port")
		dbUser     = flag.String("db-user", envOrDefault("DB_USER", "pulse"), "Database user")
		dbPassword = flag.String("db-password", envOrDefault("DB_PASSWORD", "pulse_dev"), "Database password")
		dbName     = flag.String("db-name", envOrDefault("DB_NAME", "pulse"), "Database name")

		brokers      = flag.String("brokers", envOrDefault("KAFKA_BROKERS", "localhost:9092"), "Kafka/Redpanda brokers (comma-separated)")
		pollInterval = flag.Duration("poll-interval", 100*time.Millisecond, "Polling interval for new messages")
		batchSize    = flag.Int("batch-size", 100, "Maximum messages to fetch per poll")
		workers      = flag.Int("workers", 1, "Number of publisher workers (1 for strict ordering)")

		// NATS JetStream configuration for WebSocket fanout
		natsEnabled = flag.Bool("nats-enabled", envOrDefaultBool("NATS_ENABLED", true), "Enable NATS JetStream fanout")
		natsURL     = flag.String("nats-url", envOrDefault("NATS_URL", "nats://localhost:4222"), "NATS server URL")
	)
	flag.Parse()

	// Setup structured logging
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("Starting outbox publisher",
		"brokers", *brokers,
		"poll_interval", *pollInterval,
		"batch_size", *batchSize,
		"workers", *workers,
		"nats_enabled", *natsEnabled,
		"nats_url", *natsURL,
	)

	// Create database connection
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbCfg := storage.Config{
		Host:     *dbHost,
		Port:     *dbPort,
		User:     *dbUser,
		Password: *dbPassword,
		Database: *dbName,
		SSLMode:  "disable",
	}

	db, err := storage.New(ctx, dbCfg)
	if err != nil {
		slog.Error("Failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	slog.Info("Connected to database", "host", *dbHost, "database", *dbName)

	// Create publisher
	publisher, err := NewPublisher(ctx, PublisherConfig{
		Brokers:      *brokers,
		PollInterval: *pollInterval,
		BatchSize:    *batchSize,
		Workers:      *workers,
		NATSEnabled:  *natsEnabled,
		NATSUrl:      *natsURL,
	}, db)
	if err != nil {
		slog.Error("Failed to create publisher", "error", err)
		os.Exit(1)
	}

	// Setup graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		slog.Info("Received shutdown signal", "signal", sig)
		cancel()
	}()

	// Run the publisher
	if err := publisher.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("Publisher error", "error", err)
		os.Exit(1)
	}

	slog.Info("Outbox publisher stopped")
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
		if _, err := parseIntFromString(val, &result); err == nil {
			return result
		}
	}
	return defaultVal
}

func parseIntFromString(s string, result *int) (int, error) {
	_, err := parseIntHelper(s, result)
	return *result, err
}

func parseIntHelper(s string, result *int) (int, error) {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, nil
		}
		n = n*10 + int(c-'0')
	}
	*result = n
	return n, nil
}

func envOrDefaultBool(key string, defaultVal bool) bool {
	if val := os.Getenv(key); val != "" {
		return val == "true" || val == "1" || val == "yes"
	}
	return defaultVal
}
