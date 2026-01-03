package nats

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.URL != "nats://localhost:4222" {
		t.Errorf("expected default URL nats://localhost:4222, got %s", cfg.URL)
	}
	if cfg.MaxReconnects != -1 {
		t.Errorf("expected unlimited reconnects (-1), got %d", cfg.MaxReconnects)
	}
	if cfg.ReconnectWait != 2*time.Second {
		t.Errorf("expected 2s reconnect wait, got %v", cfg.ReconnectWait)
	}
}

func TestDefaultCanonicalEventsStreamConfig(t *testing.T) {
	cfg := DefaultCanonicalEventsStreamConfig()

	if cfg.Name != "CANONICAL_EVENTS" {
		t.Errorf("expected stream name CANONICAL_EVENTS, got %s", cfg.Name)
	}
	if len(cfg.Subjects) != 1 || cfg.Subjects[0] != "events.canonical.>" {
		t.Errorf("expected subjects [events.canonical.>], got %v", cfg.Subjects)
	}
	if cfg.MaxAge != 24*time.Hour {
		t.Errorf("expected 24h max age, got %v", cfg.MaxAge)
	}
}

func TestSubjectForEvent(t *testing.T) {
	tests := []struct {
		chain     string
		eventType string
		expected  string
	}{
		{"solana", "transfer", "events.canonical.solana.transfer"},
		{"ethereum", "swap", "events.canonical.ethereum.swap"},
		{"polygon", "mint", "events.canonical.polygon.mint"},
	}

	for _, tt := range tests {
		got := SubjectForEvent(tt.chain, tt.eventType)
		if got != tt.expected {
			t.Errorf("SubjectForEvent(%q, %q) = %q, want %q", tt.chain, tt.eventType, got, tt.expected)
		}
	}
}

func TestSubjectForChain(t *testing.T) {
	got := SubjectForChain("solana")
	expected := "events.canonical.solana.>"
	if got != expected {
		t.Errorf("SubjectForChain(solana) = %q, want %q", got, expected)
	}
}

func TestDefaultFanoutConsumerConfig(t *testing.T) {
	cfg := DefaultFanoutConsumerConfig("test-consumer")

	if cfg.Name != "test-consumer" {
		t.Errorf("expected consumer name test-consumer, got %s", cfg.Name)
	}
	if !cfg.Durable {
		t.Error("expected durable consumer")
	}
	if cfg.MaxDeliver != 3 {
		t.Errorf("expected max deliver 3, got %d", cfg.MaxDeliver)
	}
}
