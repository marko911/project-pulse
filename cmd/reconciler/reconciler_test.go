package main

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/marko911/project-pulse/internal/platform/goldensource"
	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

func TestDefaultReconcilerConfig(t *testing.T) {
	cfg := DefaultReconcilerConfig()

	if cfg.ReconcileInterval != 30*time.Second {
		t.Errorf("expected ReconcileInterval 30s, got %v", cfg.ReconcileInterval)
	}
	if cfg.BatchSize != 100 {
		t.Errorf("expected BatchSize 100, got %d", cfg.BatchSize)
	}
	if cfg.LookbackBlocks != 1000 {
		t.Errorf("expected LookbackBlocks 1000, got %d", cfg.LookbackBlocks)
	}
	if !cfg.FailClosed {
		t.Error("expected FailClosed to be true by default")
	}
	if cfg.MetricsAddr != ":9093" {
		t.Errorf("expected MetricsAddr :9093, got %s", cfg.MetricsAddr)
	}
}

func TestReconciliationResult_Fields(t *testing.T) {
	result := &ReconciliationResult{
		Chain:        protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:  12345678,
		BlockHash:    "0xabc123",
		Matched:      true,
		Errors:       nil,
		ReconciledAt: time.Now(),
	}

	if result.Chain != protov1.Chain_CHAIN_ETHEREUM {
		t.Errorf("expected ETHEREUM chain, got %v", result.Chain)
	}
	if result.BlockNumber != 12345678 {
		t.Errorf("expected block 12345678, got %d", result.BlockNumber)
	}
	if !result.Matched {
		t.Error("expected Matched to be true")
	}
}

func TestReconciliationResult_Mismatch(t *testing.T) {
	result := &ReconciliationResult{
		Chain:        protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:  100,
		BlockHash:    "0xlocal",
		Matched:      false,
		Errors:       []string{"block hash mismatch: local=0xlocal golden=0xgolden"},
		ReconciledAt: time.Now(),
	}

	if result.Matched {
		t.Error("expected Matched to be false")
	}
	if len(result.Errors) != 1 {
		t.Errorf("expected 1 error, got %d", len(result.Errors))
	}
}

func TestReconcilerStats_Initial(t *testing.T) {
	stats := &ReconcilerStats{}

	if stats.BlocksReconciled != 0 {
		t.Errorf("expected BlocksReconciled 0, got %d", stats.BlocksReconciled)
	}
	if stats.BlocksMatched != 0 {
		t.Errorf("expected BlocksMatched 0, got %d", stats.BlocksMatched)
	}
	if stats.BlocksMismatched != 0 {
		t.Errorf("expected BlocksMismatched 0, got %d", stats.BlocksMismatched)
	}
}

func TestReconciler_IsHalted_Initially(t *testing.T) {
	logger := slog.Default()
	verifier := goldensource.NewVerifier(goldensource.DefaultVerifierConfig(), logger)

	cfg := DefaultReconcilerConfig()
	r := NewReconciler(cfg, nil, verifier, logger)

	if r.IsHalted() {
		t.Error("expected reconciler to not be halted initially")
	}
	if r.HaltReason() != "" {
		t.Errorf("expected empty halt reason, got %s", r.HaltReason())
	}
}

func TestReconciler_ResolveHalt_NotHalted(t *testing.T) {
	logger := slog.Default()
	verifier := goldensource.NewVerifier(goldensource.DefaultVerifierConfig(), logger)

	cfg := DefaultReconcilerConfig()
	r := NewReconciler(cfg, nil, verifier, logger)

	err := r.ResolveHalt("test resolution")
	if err == nil {
		t.Error("expected error when resolving non-halted reconciler")
	}
}

func TestReconciler_Stats(t *testing.T) {
	logger := slog.Default()
	verifier := goldensource.NewVerifier(goldensource.DefaultVerifierConfig(), logger)

	cfg := DefaultReconcilerConfig()
	r := NewReconciler(cfg, nil, verifier, logger)

	stats := r.Stats()
	if stats.BlocksReconciled != 0 {
		t.Errorf("expected 0 blocks reconciled, got %d", stats.BlocksReconciled)
	}
}

func TestReconciler_FailClosedBehavior(t *testing.T) {
	logger := slog.Default()
	verifier := goldensource.NewVerifier(goldensource.DefaultVerifierConfig(), logger)

	cfg := DefaultReconcilerConfig()
	cfg.FailClosed = true
	r := NewReconciler(cfg, nil, verifier, logger)

	r.mu.Lock()
	r.halted = true
	r.haltReason = "test mismatch"
	r.mu.Unlock()

	if !r.IsHalted() {
		t.Error("expected reconciler to be halted")
	}
	if r.HaltReason() != "test mismatch" {
		t.Errorf("expected 'test mismatch', got %s", r.HaltReason())
	}

	err := r.ResolveHalt("issue fixed")
	if err != nil {
		t.Errorf("failed to resolve: %v", err)
	}

	if r.IsHalted() {
		t.Error("expected reconciler to be resumed after resolve")
	}
}

func TestReconciler_Shutdown(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()
	verifier := goldensource.NewVerifier(goldensource.DefaultVerifierConfig(), logger)

	cfg := DefaultReconcilerConfig()
	cfg.MetricsAddr = ""
	r := NewReconciler(cfg, nil, verifier, logger)

	err := r.Shutdown(ctx)
	if err != nil {
		t.Errorf("shutdown failed: %v", err)
	}
}
