// Command processor runs the core event processing service.
//
// This service consumes raw blockchain events from adapters, normalizes them
// to the canonical CanonicalEvent protobuf format, and publishes them to the
// output stream for downstream consumers.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mirador/pulse/internal/processor"
)

func main() {
	// Configuration flags
	brokerEndpoint := flag.String("broker", getEnv("BROKER_ENDPOINT", "localhost:9092"), "Redpanda/Kafka broker endpoint")
	inputTopic := flag.String("input-topic", getEnv("INPUT_TOPIC", "raw-events"), "Topic to consume raw events from")
	outputTopic := flag.String("output-topic", getEnv("OUTPUT_TOPIC", "canonical-events"), "Topic to publish canonical events to")
	workers := flag.Int("workers", getEnvInt("WORKER_COUNT", 4), "Number of processing workers")
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

	logger.Info("starting processor",
		"broker", *brokerEndpoint,
		"input_topic", *inputTopic,
		"output_topic", *outputTopic,
		"workers", *workers,
	)

	// Create processor configuration
	cfg := processor.Config{
		WorkerCount:    *workers,
		BufferSize:     10000,
		BrokerEndpoint: *brokerEndpoint,
		InputTopic:     *inputTopic,
		OutputTopic:    *outputTopic,
	}

	_ = cfg // TODO: Use config to create processor instance

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

	// TODO: Initialize Kafka/Redpanda consumer
	// TODO: Initialize processor with normalizers
	// TODO: Start processing loop

	logger.Info("processor running, waiting for events...")

	// Wait for shutdown
	<-ctx.Done()

	// Graceful shutdown
	logger.Info("processor shutdown complete")
}

// getEnv returns environment variable value or default.
func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// getEnvInt returns environment variable as int or default.
func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		var result int
		if _, err := fmt.Sscanf(val, "%d", &result); err == nil {
			return result
		}
	}
	return defaultVal
}
