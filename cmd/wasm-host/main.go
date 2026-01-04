// Command wasm-host runs the WASM function execution host.
//
// This service consumes function invocation requests from Kafka, loads WASM
// modules from S3/MinIO, executes them using wasmtime, and returns results.
// It provides resource enforcement (CPU time limits, memory limits) and
// host SDK functions for logging and KV access.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mirador/pulse/internal/wasm"
)

func main() {
	// Configuration flags
	brokerEndpoint := flag.String("broker", getEnv("BROKER_ENDPOINT", "localhost:9092"), "Redpanda/Kafka broker endpoint")
	invocationTopic := flag.String("invocation-topic", getEnv("INVOCATION_TOPIC", "function-invocations"), "Topic to consume invocation requests from")
	resultTopic := flag.String("result-topic", getEnv("RESULT_TOPIC", "function-results"), "Topic to publish execution results to")
	billingTopic := flag.String("billing-topic", getEnv("BILLING_TOPIC", "billing-events"), "Topic to publish billing/metering events to")
	consumerGroup := flag.String("consumer-group", getEnv("CONSUMER_GROUP", "wasm-host"), "Kafka consumer group name")

	// S3/MinIO configuration
	s3Endpoint := flag.String("s3-endpoint", getEnv("S3_ENDPOINT", "localhost:9000"), "S3/MinIO endpoint")
	s3Bucket := flag.String("s3-bucket", getEnv("S3_BUCKET", "wasm-modules"), "S3 bucket for WASM modules")
	s3AccessKey := flag.String("s3-access-key", getEnv("S3_ACCESS_KEY", "minioadmin"), "S3 access key")
	s3SecretKey := flag.String("s3-secret-key", getEnv("S3_SECRET_KEY", "minioadmin"), "S3 secret key")
	s3UseSSL := flag.Bool("s3-use-ssl", getEnvBool("S3_USE_SSL", false), "Use SSL for S3 connection")

	// Redis configuration (for KV store)
	redisAddr := flag.String("redis-addr", getEnv("REDIS_ADDR", "localhost:6379"), "Redis address for KV store")

	// Runtime configuration
	workers := flag.Int("workers", getEnvInt("WORKER_COUNT", 4), "Number of execution workers")
	maxMemoryMB := flag.Int("max-memory-mb", getEnvInt("MAX_MEMORY_MB", 128), "Maximum memory per WASM instance in MB")
	maxCPUMs := flag.Int("max-cpu-ms", getEnvInt("MAX_CPU_MS", 5000), "Maximum CPU time per invocation in milliseconds")
	moduleCacheSize := flag.Int("module-cache-size", getEnvInt("MODULE_CACHE_SIZE", 100), "Number of compiled modules to cache")

	// Logging
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

	logger.Info("starting wasm-host",
		"broker", *brokerEndpoint,
		"invocation_topic", *invocationTopic,
		"result_topic", *resultTopic,
		"billing_topic", *billingTopic,
		"consumer_group", *consumerGroup,
		"s3_endpoint", *s3Endpoint,
		"s3_bucket", *s3Bucket,
		"redis_addr", *redisAddr,
		"workers", *workers,
		"max_memory_mb", *maxMemoryMB,
		"max_cpu_ms", *maxCPUMs,
		"module_cache_size", *moduleCacheSize,
	)

	// Create host configuration
	cfg := wasm.HostConfig{
		BrokerEndpoint:  *brokerEndpoint,
		InvocationTopic: *invocationTopic,
		ResultTopic:     *resultTopic,
		BillingTopic:    *billingTopic,
		ConsumerGroup:   *consumerGroup,
		S3Endpoint:      *s3Endpoint,
		S3Bucket:        *s3Bucket,
		S3AccessKey:     *s3AccessKey,
		S3SecretKey:     *s3SecretKey,
		S3UseSSL:        *s3UseSSL,
		RedisAddr:       *redisAddr,
		WorkerCount:     *workers,
		MaxMemoryMB:     *maxMemoryMB,
		MaxCPUMs:        *maxCPUMs,
		ModuleCacheSize: *moduleCacheSize,
	}

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

	// Create and start the WASM host
	host, err := wasm.NewHost(cfg, logger)
	if err != nil {
		logger.Error("failed to create wasm host", "error", err)
		os.Exit(1)
	}

	// Start the host (blocks until context is cancelled)
	if err := host.Run(ctx); err != nil {
		logger.Error("wasm host error", "error", err)
		os.Exit(1)
	}

	// Graceful shutdown
	logger.Info("wasm-host shutdown complete")
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

// getEnvBool returns environment variable as bool or default.
func getEnvBool(key string, defaultVal bool) bool {
	if val := os.Getenv(key); val != "" {
		return val == "true" || val == "1" || val == "yes"
	}
	return defaultVal
}
