package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

func TestDetector_NewDetector(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := DetectorConfig{
		Brokers:       []string{"localhost:9092"},
		Topics:        []string{"test-topic"},
		ConsumerGroup: "test-group",
		PollInterval:  100 * time.Millisecond,
		StateDir:      "/tmp/gap-detector-test",
		MetricsAddr:   ":0", // Random port
	}

	detector, err := NewDetector(cfg, logger)
	if err != nil {
		t.Fatalf("failed to create detector: %v", err)
	}

	if detector.state == nil {
		t.Error("expected state to be initialized")
	}

	if detector.IsHalted() {
		t.Error("expected detector to not be halted initially")
	}
}

func TestDetector_Halt(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := DetectorConfig{
		Brokers:       []string{"localhost:9092"},
		Topics:        []string{"test-topic"},
		ConsumerGroup: "test-group",
	}

	detector, _ := NewDetector(cfg, logger)

	if detector.IsHalted() {
		t.Error("expected detector to not be halted initially")
	}

	// Halt the detector
	detector.halt(nil)

	if !detector.IsHalted() {
		t.Error("expected detector to be halted after halt()")
	}
}

func TestDetector_HandleGap_HaltsWithZeroThreshold(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := DetectorConfig{
		Brokers:      []string{"localhost:9092"},
		Topics:       []string{"test-topic"},
		MaxGapBlocks: 0, // Any gap triggers halt
	}

	detector, _ := NewDetector(cfg, logger)

	gap := &GapEvent{
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: 3,
		ExpectedBlock:   100,
		ReceivedBlock:   102,
		GapSize:         2,
		DetectedAt:      time.Now(),
		Topic:           "test-topic",
	}

	detector.handleGap(context.Background(), gap)

	if !detector.IsHalted() {
		t.Error("expected detector to halt on gap with zero threshold")
	}
}

func TestDetector_HandleGap_DoesNotHaltWithinThreshold(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := DetectorConfig{
		Brokers:      []string{"localhost:9092"},
		Topics:       []string{"test-topic"},
		MaxGapBlocks: 5, // Allow gaps up to 5 blocks
	}

	detector, _ := NewDetector(cfg, logger)

	gap := &GapEvent{
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: 3,
		ExpectedBlock:   100,
		ReceivedBlock:   102,
		GapSize:         2, // Within threshold
		DetectedAt:      time.Now(),
		Topic:           "test-topic",
	}

	detector.handleGap(context.Background(), gap)

	if detector.IsHalted() {
		t.Error("expected detector to NOT halt when gap is within threshold")
	}
}

func TestDetector_HandleGap_HaltsAboveThreshold(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := DetectorConfig{
		Brokers:      []string{"localhost:9092"},
		Topics:       []string{"test-topic"},
		MaxGapBlocks: 5,
	}

	detector, _ := NewDetector(cfg, logger)

	gap := &GapEvent{
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: 3,
		ExpectedBlock:   100,
		ReceivedBlock:   110,
		GapSize:         10, // Above threshold
		DetectedAt:      time.Now(),
		Topic:           "test-topic",
	}

	detector.handleGap(context.Background(), gap)

	if !detector.IsHalted() {
		t.Error("expected detector to halt when gap exceeds threshold")
	}
}

func TestDetector_HealthEndpoint(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := DetectorConfig{
		Brokers: []string{"localhost:9092"},
		Topics:  []string{"test-topic"},
	}

	detector, _ := NewDetector(cfg, logger)

	// Create a test server with the health handler
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		detector.mu.Lock()
		halted := detector.halted
		detector.mu.Unlock()

		if halted {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"status":"halted","reason":"gap_detected"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy"}`))
	})

	// Test healthy state
	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	// Halt and test again
	detector.halt(nil)

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status 503 when halted, got %d", rec.Code)
	}
}

func TestDetector_MetricsEndpoint(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := DetectorConfig{
		Brokers: []string{"localhost:9092"},
		Topics:  []string{"test-topic"},
	}

	detector, _ := NewDetector(cfg, logger)

	// Simulate some processing
	detector.mu.Lock()
	detector.blocksScanned = 100
	detector.gapCount = 2
	detector.mu.Unlock()

	// Create metrics handler
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		detector.mu.Lock()
		metrics := map[string]interface{}{
			"blocks_scanned": detector.blocksScanned,
			"gap_count":      detector.gapCount,
			"halted":         detector.halted,
			"chains":         detector.state.GetStats(),
		}
		detector.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(metrics)
	})

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var metrics map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&metrics); err != nil {
		t.Fatalf("failed to decode metrics: %v", err)
	}

	if metrics["blocks_scanned"].(float64) != 100 {
		t.Errorf("expected blocks_scanned=100, got %v", metrics["blocks_scanned"])
	}
}

func TestGapEvent_JSON(t *testing.T) {
	gap := GapEvent{
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: 3,
		ExpectedBlock:   100,
		ReceivedBlock:   105,
		GapSize:         5,
		DetectedAt:      time.Now(),
		Topic:           "finalized-events",
	}

	data, err := json.Marshal(gap)
	if err != nil {
		t.Fatalf("failed to marshal GapEvent: %v", err)
	}

	var decoded GapEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal GapEvent: %v", err)
	}

	if decoded.GapSize != gap.GapSize {
		t.Errorf("expected GapSize=%d, got %d", gap.GapSize, decoded.GapSize)
	}
	if decoded.Chain != gap.Chain {
		t.Errorf("expected Chain=%d, got %d", gap.Chain, decoded.Chain)
	}
}
