// +build integration

package nats_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pnats "github.com/marko911/project-pulse/internal/platform/nats"
)

func TestNATSIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := pnats.DefaultConfig()
	cfg.URL = "nats://localhost:4222"
	cfg.Name = "integration-test"

	client, err := pnats.Connect(ctx, cfg)
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer client.Close()

	t.Log("Successfully connected to NATS")

	streamCfg := pnats.DefaultCanonicalEventsStreamConfig()
	stream, err := pnats.EnsureStream(ctx, client.JetStream(), streamCfg)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	t.Logf("Created stream: %s", streamCfg.Name)

	consumerCfg := pnats.DefaultFanoutConsumerConfig("integration-test-consumer")
	consumer, err := pnats.EnsureConsumer(ctx, stream, consumerCfg)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	t.Logf("Created consumer: %s", consumerCfg.Name)

	testEvent := map[string]interface{}{
		"event_id":   "test-event-001",
		"chain":      "solana",
		"event_type": "transfer",
		"block_num":  12345,
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
	}

	eventData, err := json.Marshal(testEvent)
	if err != nil {
		t.Fatalf("Failed to marshal event: %v", err)
	}

	subject := pnats.SubjectForEvent("solana", "transfer")
	ack, err := client.JetStream().Publish(ctx, subject, eventData)
	if err != nil {
		t.Fatalf("Failed to publish event: %v", err)
	}

	t.Logf("Published event to %s, seq=%d", subject, ack.Sequence)

	msgs, err := consumer.Fetch(1)
	if err != nil {
		t.Fatalf("Failed to fetch messages: %v", err)
	}

	msgCount := 0
	for msg := range msgs.Messages() {
		var received map[string]interface{}
		if err := json.Unmarshal(msg.Data(), &received); err != nil {
			t.Errorf("Failed to unmarshal received message: %v", err)
		} else {
			t.Logf("Received event: %v", received["event_id"])
			if received["event_id"] != testEvent["event_id"] {
				t.Errorf("Event ID mismatch: got %v, want %v", received["event_id"], testEvent["event_id"])
			}
		}
		msg.Ack()
		msgCount++
	}

	if msgCount != 1 {
		t.Errorf("Expected 1 message, got %d", msgCount)
	}

	t.Log("NATS JetStream integration test passed!")
}
