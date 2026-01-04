// Package main implements the Reconciliation Service.
// This service compares local manifests with golden source data
// to verify data integrity and implements fail-closed behavior.
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

	"github.com/mirador/pulse/internal/platform/goldensource"
	"github.com/mirador/pulse/internal/platform/storage"
	protov1 "github.com/mirador/pulse/pkg/proto/v1"
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

		// Golden source configuration
		goldenEVMURL    = flag.String("golden-evm-url", envOrDefault("GOLDEN_EVM_URL", ""), "Golden source EVM RPC URL")
		goldenSolanaURL = flag.String("golden-solana-url", envOrDefault("GOLDEN_SOLANA_URL", ""), "Golden source Solana RPC URL")

		// Reconciler configuration
		reconcileInterval = flag.Duration("reconcile-interval", 30*time.Second, "Reconciliation interval")
		batchSize         = flag.Int("batch-size", 100, "Max blocks per reconcile cycle")
		lookbackBlocks    = flag.Uint64("lookback-blocks", 1000, "How far back to look for unreconciled blocks")
		failClosed        = flag.Bool("fail-closed", true, "Halt on reconciliation mismatch")

		// Service configuration
		metricsAddr = flag.String("metrics-addr", envOrDefault("METRICS_ADDR", ":9093"), "Address for metrics endpoint")
		logLevel    = flag.String("log-level", envOrDefault("LOG_LEVEL", "info"), "Log level: debug, info, warn, error")
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

	slog.Info("starting reconciler",
		"reconcile_interval", *reconcileInterval,
		"batch_size", *batchSize,
		"lookback_blocks", *lookbackBlocks,
		"fail_closed", *failClosed,
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

	// Create golden source verifier
	verifierCfg := goldensource.DefaultVerifierConfig()
	verifierCfg.FailClosed = *failClosed
	verifier := goldensource.NewVerifier(verifierCfg, logger)

	// Register golden source clients
	if *goldenEVMURL != "" {
		evmClient := goldensource.NewEVMClient(&goldensource.Config{
			URL:   *goldenEVMURL,
			Chain: protov1.Chain_CHAIN_ETHEREUM,
			Name:  "evm-golden",
		}, logger)
		verifier.RegisterClient(evmClient)

		if err := evmClient.Connect(ctx); err != nil {
			slog.Warn("failed to connect EVM golden source", "error", err)
		}
	}

	// Note: Solana client would be registered similarly
	if *goldenSolanaURL != "" {
		slog.Info("solana golden source URL configured (client not yet implemented)",
			"url", *goldenSolanaURL,
		)
	}

	// Create and run the reconciler
	reconcilerCfg := ReconcilerConfig{
		ReconcileInterval: *reconcileInterval,
		BatchSize:         *batchSize,
		LookbackBlocks:    *lookbackBlocks,
		FailClosed:        *failClosed,
		MetricsAddr:       *metricsAddr,
	}

	reconciler := NewReconciler(reconcilerCfg, db, verifier, logger)

	// Setup graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		slog.Info("received shutdown signal", "signal", sig)
		cancel()
	}()

	// Run the reconciler
	if err := reconciler.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("reconciler error", "error", err)
		os.Exit(1)
	}

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := reconciler.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "error", err)
	}

	if err := verifier.CloseAll(); err != nil {
		slog.Error("verifier close error", "error", err)
	}

	slog.Info("reconciler shutdown complete")
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

// splitAndTrim splits a comma-separated string and trims whitespace.
func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
