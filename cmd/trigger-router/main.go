package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var (
		brokers       = flag.String("brokers", envOrDefault("KAFKA_BROKERS", "localhost:9092"), "Kafka/Redpanda brokers (comma-separated)")
		inputTopic    = flag.String("input-topic", envOrDefault("INPUT_TOPIC", "canonical-events"), "Topic to consume canonical events from")
		outputTopic   = flag.String("output-topic", envOrDefault("OUTPUT_TOPIC", "function-invocations"), "Topic to publish function invocations to")
		consumerGroup = flag.String("consumer-group", envOrDefault("CONSUMER_GROUP", "trigger-router"), "Kafka consumer group name")

		redisAddr     = flag.String("redis-addr", envOrDefault("REDIS_ADDR", "localhost:6379"), "Redis server address")
		redisPassword = flag.String("redis-password", envOrDefault("REDIS_PASSWORD", ""), "Redis password")
		redisDB       = flag.Int("redis-db", envOrDefaultInt("REDIS_DB", 0), "Redis database number")
		redisPrefix   = flag.String("redis-prefix", envOrDefault("REDIS_PREFIX", "triggers:"), "Redis key prefix for triggers")

		workers  = flag.Int("workers", envOrDefaultInt("WORKER_COUNT", 4), "Number of processing workers")
		logLevel = flag.String("log-level", envOrDefault("LOG_LEVEL", "info"), "Log level: debug, info, warn, error")
	)
	flag.Parse()

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

	slog.Info("Starting trigger-router",
		"brokers", *brokers,
		"input_topic", *inputTopic,
		"output_topic", *outputTopic,
		"consumer_group", *consumerGroup,
		"redis_addr", *redisAddr,
		"workers", *workers,
	)

	cfg := RouterConfig{
		Brokers:       *brokers,
		InputTopic:    *inputTopic,
		OutputTopic:   *outputTopic,
		ConsumerGroup: *consumerGroup,
		RedisAddr:     *redisAddr,
		RedisPassword: *redisPassword,
		RedisDB:       *redisDB,
		RedisPrefix:   *redisPrefix,
		Workers:       *workers,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		slog.Info("Received shutdown signal", "signal", sig)
		cancel()
	}()

	router, err := NewRouter(ctx, cfg)
	if err != nil {
		slog.Error("Failed to create router", "error", err)
		os.Exit(1)
	}

	if err := router.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("Router error", "error", err)
		os.Exit(1)
	}

	slog.Info("Trigger-router stopped")
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
		if _, err := fmt.Sscanf(val, "%d", &result); err == nil {
			return result
		}
	}
	return defaultVal
}
