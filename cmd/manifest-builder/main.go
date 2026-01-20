// Package main implements the Manifest Builder service.
// This service aggregates finalized block data into correctness manifests.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/marko911/project-pulse/internal/platform/storage"
)

func main() {
	// Configuration flags
	var (
		// Database configuration
		dbHost     = flag.String("db-host", envOrDefault("DB_HOST", "localhost"), "Database host")
		dbPort     = flag.Int("db-port", envOrDefaultInt("DB_PORT", 5432), "Database port")
		dbUser     = flag.String("db-user", envOrDefault("DB_USER", "pulse"), "Database user")
		dbPassword = flag.String("db-password", envOrDefault("DB_PASSWORD", "pulse_dev"), "Database password")
		dbName     = flag.String("db-name", envOrDefault("DB_NAME", "pulse"), "Database name")

		// Kafka configuration
		brokers       = flag.String("brokers", envOrDefault("KAFKA_BROKERS", "localhost:9092"), "Kafka/Redpanda brokers (comma-separated)")
		topics        = flag.String("topics", envOrDefault("TOPICS", "finalized-events"), "Topics to consume (comma-separated)")
		consumerGroup = flag.String("group", envOrDefault("CONSUMER_GROUP", "manifest-builder"), "Consumer group ID")

		// Service configuration
		pollInterval   = flag.Duration("poll-interval", 100*time.Millisecond, "Polling interval")
		flushInterval  = flag.Duration("flush-interval", 5*time.Second, "Interval to flush pending manifests")
		metricsAddr    = flag.String("metrics-addr", envOrDefault("METRICS_ADDR", ":9091"), "Address for metrics endpoint")
		logLevel       = flag.String("log-level", envOrDefault("LOG_LEVEL", "info"), "Log level: debug, info, warn, error")
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

	// Parse topics and brokers
	topicList := strings.Split(*topics, ",")
	for i := range topicList {
		topicList[i] = strings.TrimSpace(topicList[i])
	}

	brokerList := strings.Split(*brokers, ",")
	for i := range brokerList {
		brokerList[i] = strings.TrimSpace(brokerList[i])
	}

	slog.Info("starting manifest-builder",
		"brokers", brokerList,
		"topics", topicList,
		"consumer_group", *consumerGroup,
		"flush_interval", *flushInterval,
		"metrics_addr", *metricsAddr,
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
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	// Run database migrations
	if err := db.Migrate(ctx); err != nil {
		slog.Error("failed to run database migrations", "error", err)
		os.Exit(1)
	}

	slog.Info("connected to database and applied migrations", "host", *dbHost, "database", *dbName)

	// Create and run the builder
	builder, err := NewBuilder(BuilderConfig{
		Brokers:       brokerList,
		Topics:        topicList,
		ConsumerGroup: *consumerGroup,
		PollInterval:  *pollInterval,
		FlushInterval: *flushInterval,
		MetricsAddr:   *metricsAddr,
	}, db, logger)
	if err != nil {
		slog.Error("failed to create builder", "error", err)
		os.Exit(1)
	}

	// Setup graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		slog.Info("received shutdown signal", "signal", sig)
		cancel()
	}()

	// Run the builder
	if err := builder.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("builder error", "error", err)
		os.Exit(1)
	}

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := builder.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "error", err)
	}

	slog.Info("manifest-builder shutdown complete")
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
			if c >= '0' && c <= '9' {
				result = result*10 + int(c-'0')
			} else {
				return defaultVal
			}
		}
		return result
	}
	return defaultVal
}
