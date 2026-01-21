package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/marko911/project-pulse/internal/delivery/subscription"
	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

func main() {
	var cfg Config
	flag.StringVar(&cfg.ListenAddr, "listen", ":8080", "HTTP listen address")
	flag.StringVar(&cfg.DBConnString, "db", "", "PostgreSQL connection string")
	flag.DurationVar(&cfg.ReadTimeout, "read-timeout", 10*time.Second, "HTTP read timeout")
	flag.DurationVar(&cfg.WriteTimeout, "write-timeout", 10*time.Second, "HTTP write timeout")
	flag.BoolVar(&cfg.NATSEnabled, "nats-enabled", envOrDefaultBool("NATS_ENABLED", true), "Enable NATS JetStream consumer")
	flag.StringVar(&cfg.NATSURL, "nats-url", envOrDefault("NATS_URL", "nats://localhost:4222"), "NATS server URL")
	flag.StringVar(&cfg.NATSConsumerName, "nats-consumer", envOrDefault("NATS_CONSUMER_NAME", uniqueConsumerName()), "NATS consumer name (must be unique per instance for fanout)")
	flag.StringVar(&cfg.RedisAddr, "redis-addr", envOrDefault("REDIS_ADDR", "localhost:6379"), "Redis server address")
	flag.StringVar(&cfg.RedisPassword, "redis-password", envOrDefault("REDIS_PASSWORD", ""), "Redis password")
	flag.IntVar(&cfg.RedisDB, "redis-db", envOrDefaultInt("REDIS_DB", 0), "Redis database number")
	wsOrigins := flag.String("ws-allowed-origins", envOrDefault("WS_ALLOWED_ORIGINS", "*"), "Comma-separated list of allowed WebSocket origins, or '*' for all")
	flag.Parse()

	cfg.AllowedOrigins = parseOrigins(*wsOrigins)

	if v := os.Getenv("API_LISTEN_ADDR"); v != "" {
		cfg.ListenAddr = v
	}
	if v := os.Getenv("DATABASE_URL"); v != "" {
		cfg.DBConnString = v
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if err := run(cfg, logger); err != nil {
		logger.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

type Config struct {
	ListenAddr   string
	DBConnString string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	NATSEnabled      bool
	NATSURL          string
	NATSConsumerName string
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	AllowedOrigins []string
}

func run(cfg Config, logger *slog.Logger) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := NewServer(cfg, logger)

	subManager, err := subscription.NewRedisManager(subscription.RedisConfig{
		Addr:       cfg.RedisAddr,
		Password:   cfg.RedisPassword,
		DB:         cfg.RedisDB,
		KeyPrefix:  "pulse:subs:",
		DefaultTTL: 24 * time.Hour,
	})
	if err != nil {
		logger.Warn("Redis subscription manager initialization failed, WebSocket subscriptions disabled", "error", err)
	} else {
		wsHandler := NewWebSocketHandler(subManager, cfg.AllowedOrigins, logger)
		server.SetWebSocketHandler(wsHandler)
		logger.Info("WebSocket subscription system initialized",
			"redis_addr", cfg.RedisAddr,
			"allowed_origins", cfg.AllowedOrigins,
		)
	}

	var natsConsumer *NATSConsumer
	if cfg.NATSEnabled {
		consumer, err := NewNATSConsumer(ctx, NATSConsumerConfig{
			URL:          cfg.NATSURL,
			ConsumerName: cfg.NATSConsumerName,
			Logger:       logger.With("component", "nats-consumer"),
			OnEvent: func(event *protov1.CanonicalEvent) error {
				server.HandleNATSEvent(event)
				return nil
			},
		})
		if err != nil {
			logger.Warn("NATS consumer initialization failed, continuing without real-time fanout", "error", err)
		} else {
			natsConsumer = consumer
			if err := natsConsumer.Start(ctx); err != nil {
				logger.Error("failed to start NATS consumer", "error", err)
			} else {
				logger.Info("NATS JetStream consumer started",
					"url", cfg.NATSURL,
					"consumer", cfg.NATSConsumerName,
				)
			}
		}
	}

	httpServer := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      server.Router(),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("shutdown signal received")

		shutdownCtx, shutdownCancel := context.WithTimeout(ctx, 30*time.Second)
		defer shutdownCancel()

		if natsConsumer != nil {
			if err := natsConsumer.Close(); err != nil {
				logger.Error("NATS consumer shutdown error", "error", err)
			}
		}

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("HTTP server shutdown error", "error", err)
		}
		cancel()
	}()

	logger.Info("starting API gateway", "addr", cfg.ListenAddr)
	if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
		return fmt.Errorf("HTTP server error: %w", err)
	}

	return nil
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func envOrDefaultBool(key string, defaultVal bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return defaultVal
}

func envOrDefaultInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return defaultVal
}

func uniqueConsumerName() string {
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Sprintf("api-gateway-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("api-gateway-%s", hostname)
}

func parseOrigins(origins string) []string {
	origins = strings.TrimSpace(origins)
	if origins == "" || origins == "*" {
		return nil
	}

	parts := strings.Split(origins, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
