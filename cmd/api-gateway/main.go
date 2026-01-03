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
	flag.StringVar(&cfg.ListenAddr, "listen", ":8080", "HTTP listen address")
	flag.StringVar(&cfg.DBConnString, "db", "", "PostgreSQL connection string")
	flag.DurationVar(&cfg.ReadTimeout, "read-timeout", 10*time.Second, "HTTP read timeout")
	flag.DurationVar(&cfg.WriteTimeout, "write-timeout", 10*time.Second, "HTTP write timeout")
	flag.Parse()

	// Override with environment variables if set
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

// Config holds configuration for the API gateway.
type Config struct {
	ListenAddr   string
	DBConnString string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func run(cfg Config, logger *slog.Logger) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create server with handlers
	server := NewServer(cfg, logger)

	// Set up HTTP server
	httpServer := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      server.Router(),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("shutdown signal received")

		shutdownCtx, shutdownCancel := context.WithTimeout(ctx, 30*time.Second)
		defer shutdownCancel()

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
