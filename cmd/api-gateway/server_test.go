package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/marko911/project-pulse/internal/correctness"
	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

func TestNewServer(t *testing.T) {
	cfg := Config{
		ListenAddr: ":8080",
	}
	logger := slog.Default()

	server := NewServer(cfg, logger)
	if server == nil {
		t.Fatal("expected non-nil server")
	}
}

func TestServer_Router(t *testing.T) {
	server := NewServer(Config{}, slog.Default())
	router := server.Router()
	if router == nil {
		t.Fatal("expected non-nil router")
	}
}

func TestServer_HealthEndpoint(t *testing.T) {
	server := NewServer(Config{}, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var response map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if response["status"] != "healthy" {
		t.Errorf("expected status 'healthy', got %v", response["status"])
	}
}

func TestServer_ReadyEndpoint(t *testing.T) {
	server := NewServer(Config{}, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var response map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if response["ready"] != true {
		t.Errorf("expected ready=true, got %v", response["ready"])
	}
}

func TestServer_ReadyEndpoint_WhenHalted(t *testing.T) {
	server := NewServer(Config{}, slog.Default())

	// Create and configure watermark controller
	wc := correctness.NewWatermarkController(correctness.DefaultWatermarkConfig(), slog.Default())
	wc.InitializeChain(1, 100) // Initialize with chain 1
	wc.Halt(correctness.HaltSourceManualHold, 1, 100, "test halt")
	server.SetWatermarkController(wc)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", rec.Code)
	}

	var response map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if response["ready"] != false {
		t.Errorf("expected ready=false, got %v", response["ready"])
	}
}

func TestServer_CorrectnessStatusEndpoint(t *testing.T) {
	server := NewServer(Config{}, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/correctness/status", nil)
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestServer_CorrectnessStatusEndpoint_WithWatermark(t *testing.T) {
	server := NewServer(Config{}, slog.Default())
	wc := correctness.NewWatermarkController(correctness.DefaultWatermarkConfig(), slog.Default())
	server.SetWatermarkController(wc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/correctness/status", nil)
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var response map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if _, ok := response["watermark"]; !ok {
		t.Error("expected watermark in response")
	}
}

func TestServer_HaltsEndpoint(t *testing.T) {
	server := NewServer(Config{}, slog.Default())
	wc := correctness.NewWatermarkController(correctness.DefaultWatermarkConfig(), slog.Default())
	server.SetWatermarkController(wc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/correctness/halts", nil)
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var response map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if response["halted"] != false {
		t.Errorf("expected halted=false, got %v", response["halted"])
	}
}

func TestServer_HaltsEndpoint_NoController(t *testing.T) {
	server := NewServer(Config{}, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/correctness/halts", nil)
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	// handlers.go returns 200 with available:false for no controller
	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestServer_GapsEndpoint(t *testing.T) {
	server := NewServer(Config{}, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/correctness/gaps", nil)
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestServer_ManifestsEndpoint_NoDB(t *testing.T) {
	server := NewServer(Config{}, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/manifests/ethereum", nil)
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", rec.Code)
	}
}

func TestServer_ManifestsEndpoint_InvalidChain(t *testing.T) {
	server := NewServer(Config{}, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/manifests/invalidchain", nil)
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

func TestProtoChainFromName(t *testing.T) {
	tests := []struct {
		input    string
		expected protov1.Chain
	}{
		{"ethereum", protov1.Chain_CHAIN_ETHEREUM},
		{"eth", protov1.Chain_CHAIN_ETHEREUM},
		{"solana", protov1.Chain_CHAIN_SOLANA},
		{"sol", protov1.Chain_CHAIN_SOLANA},
		{"polygon", protov1.Chain_CHAIN_POLYGON},
		{"matic", protov1.Chain_CHAIN_POLYGON},
		{"arbitrum", protov1.Chain_CHAIN_ARBITRUM},
		{"arb", protov1.Chain_CHAIN_ARBITRUM},
		{"optimism", protov1.Chain_CHAIN_OPTIMISM},
		{"op", protov1.Chain_CHAIN_OPTIMISM},
		{"base", protov1.Chain_CHAIN_BASE},
		{"avalanche", protov1.Chain_CHAIN_AVALANCHE},
		{"avax", protov1.Chain_CHAIN_AVALANCHE},
		{"bsc", protov1.Chain_CHAIN_BSC},
		{"bnb", protov1.Chain_CHAIN_BSC},
		{"invalid", protov1.Chain_CHAIN_UNSPECIFIED},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			result := protoChainFromName(tc.input)
			if result != tc.expected {
				t.Errorf("protoChainFromName(%q) = %v, want %v", tc.input, result, tc.expected)
			}
		})
	}
}

func TestServer_CorrectnessStatusEndpoint_MethodNotAllowed(t *testing.T) {
	server := NewServer(Config{}, slog.Default())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/correctness/status", nil)
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", rec.Code)
	}
}
