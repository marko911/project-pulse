package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	var cfg Config
	flag.StringVar(&cfg.ListenAddr, "listen", ":8090", "HTTP listen address")
	flag.StringVar(&cfg.MinIOEndpoint, "minio-endpoint", envOrDefault("MINIO_ENDPOINT", "localhost:9000"), "MinIO/S3 endpoint")
	flag.StringVar(&cfg.MinIOAccessKey, "minio-access-key", envOrDefault("MINIO_ACCESS_KEY", "pulse"), "MinIO access key")
	flag.StringVar(&cfg.MinIOSecretKey, "minio-secret-key", envOrDefault("MINIO_SECRET_KEY", "pulse_dev_123"), "MinIO secret key")
	flag.StringVar(&cfg.MinIOBucket, "minio-bucket", envOrDefault("MINIO_BUCKET", "wasm-modules"), "MinIO bucket for WASM modules")
	flag.BoolVar(&cfg.MinIOUseSSL, "minio-ssl", envOrDefaultBool("MINIO_USE_SSL", false), "Use SSL for MinIO")
	flag.StringVar(&cfg.RedisAddr, "redis-addr", envOrDefault("REDIS_ADDR", "localhost:6379"), "Redis address")
	flag.StringVar(&cfg.RedisPassword, "redis-password", envOrDefault("REDIS_PASSWORD", ""), "Redis password")

	flag.StringVar(&cfg.DBHost, "db-host", envOrDefault("DB_HOST", "localhost"), "Database host")
	flag.IntVar(&cfg.DBPort, "db-port", envOrDefaultInt("DB_PORT", 5432), "Database port")
	flag.StringVar(&cfg.DBUser, "db-user", envOrDefault("DB_USER", "pulse"), "Database user")
	flag.StringVar(&cfg.DBPassword, "db-password", envOrDefault("DB_PASSWORD", "pulse_dev"), "Database password")
	flag.StringVar(&cfg.DBName, "db-name", envOrDefault("DB_NAME", "pulse"), "Database name")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if err := run(cfg, logger); err != nil {
		logger.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

type Config struct {
	ListenAddr     string
	MinIOEndpoint  string
	MinIOAccessKey string
	MinIOSecretKey string
	MinIOBucket    string
	MinIOUseSSL    bool
	RedisAddr      string
	RedisPassword  string

	DBHost     string
	DBPort     int
	DBUser     string
	DBPassword string
	DBName     string
}

func run(cfg Config, logger *slog.Logger) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server, err := NewServer(ctx, cfg, logger)
	if err != nil {
		return fmt.Errorf("create server: %w", err)
	}
	defer server.Close()

	httpServer := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      server.Router(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("shutdown signal received")
		shutdownCtx, shutdownCancel := context.WithTimeout(ctx, 30*time.Second)
		defer shutdownCancel()
		httpServer.Shutdown(shutdownCtx)
		cancel()
	}()

	logger.Info("starting function API", "addr", cfg.ListenAddr)
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
		return v == "true" || v == "1" || v == "yes"
	}
	return defaultVal
}

func envOrDefaultInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		var result int
		if _, err := fmt.Sscanf(v, "%d", &result); err == nil {
			return result
		}
	}
	return defaultVal
}
