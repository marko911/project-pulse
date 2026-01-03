// Package main is the entrypoint for the EVM adapter service.
// The EVM adapter connects to EVM-compatible chains (Ethereum, Polygon, Arbitrum, etc.)
// via WebSocket or HTTP RPC, ingests blocks and events, normalizes them to the canonical
// event format, and publishes them to the broker.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mirador/pulse/internal/adapter/evm"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "", "path to configuration file")
	chain := flag.String("chain", "ethereum", "chain to connect to (ethereum, polygon, arbitrum, optimism, base, avalanche, bsc)")
	rpcURL := flag.String("rpc", "", "RPC endpoint URL (WebSocket or HTTP)")
	logLevel := flag.String("log-level", "info", "log level (debug, info, warn, error)")
	flag.Parse()

	// Setup structured logging
	level := parseLogLevel(*logLevel)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	logger.Info("starting EVM adapter",
		"chain", *chain,
		"config", *configPath,
	)

	// Load configuration
	cfg, err := evm.LoadConfig(*configPath, *chain, *rpcURL)
	if err != nil {
		logger.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	// Create adapter
	adapter, err := evm.NewAdapter(cfg, logger)
	if err != nil {
		logger.Error("failed to create adapter", "error", err)
		os.Exit(1)
	}

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		logger.Info("received shutdown signal", "signal", sig)
		cancel()
	}()

	// Run the adapter
	if err := adapter.Run(ctx); err != nil {
		logger.Error("adapter exited with error", "error", err)
		os.Exit(1)
	}

	logger.Info("EVM adapter shutdown complete")
}

func parseLogLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
