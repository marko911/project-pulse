package storage

import (
	"context"
	"testing"
	"time"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

func TestOutboxRepository_SaveEventWithOutbox(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	cfg := DefaultConfig()

	db, err := New(ctx, cfg)
	if err != nil {
		t.Skipf("Cannot connect to database: %v", err)
	}
	defer db.Close()

	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("Migrate failed: %v", err)
	}

	repo := NewOutboxRepository(db)

	event := &protov1.CanonicalEvent{
		EventId:         "test-event-001",
		Chain:           protov1.Chain_CHAIN_SOLANA,
		CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_CONFIRMED,
		BlockNumber:     12345,
		BlockHash:       "abc123",
		TxHash:          "tx456",
		TxIndex:         0,
		EventIndex:      0,
		EventType:       "Transfer",
		Accounts:        []string{"account1", "account2"},
		Timestamp:       time.Now(),
		IngestedAt:      time.Now(),
		ReorgAction:     protov1.ReorgAction_REORG_ACTION_NORMAL,
		SchemaVersion:   1,
	}

	err = repo.SaveEventWithOutbox(ctx, event, "test-topic")
	if err != nil {
		t.Fatalf("SaveEventWithOutbox failed: %v", err)
	}

	saved, err := repo.GetEventByID(ctx, event.EventId)
	if err != nil {
		t.Fatalf("GetEventByID failed: %v", err)
	}
	if saved == nil {
		t.Fatal("Event not found after save")
	}
	if saved.EventID != event.EventId {
		t.Errorf("Expected event ID %s, got %s", event.EventId, saved.EventID)
	}

	pending, err := repo.FetchPendingMessages(ctx, 10)
	if err != nil {
		t.Fatalf("FetchPendingMessages failed: %v", err)
	}

	found := false
	for _, msg := range pending {
		if msg.EventID == event.EventId {
			found = true
			if msg.Topic != "test-topic" {
				t.Errorf("Expected topic test-topic, got %s", msg.Topic)
			}
			if msg.Status != OutboxStatusPending {
				t.Errorf("Expected status pending, got %s", msg.Status)
			}
			break
		}
	}
	if !found {
		t.Error("Outbox message not found for event")
	}
}

func TestOutboxRepository_MarkAsPublished(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	cfg := DefaultConfig()

	db, err := New(ctx, cfg)
	if err != nil {
		t.Skipf("Cannot connect to database: %v", err)
	}
	defer db.Close()

	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("Migrate failed: %v", err)
	}

	repo := NewOutboxRepository(db)

	event := &protov1.CanonicalEvent{
		EventId:         "test-event-002-" + time.Now().Format(time.RFC3339Nano),
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
		BlockNumber:     100,
		BlockHash:       "block100",
		TxHash:          "tx100",
		Timestamp:       time.Now(),
		IngestedAt:      time.Now(),
		EventType:       "Swap",
		ReorgAction:     protov1.ReorgAction_REORG_ACTION_NORMAL,
		SchemaVersion:   1,
	}

	if err := repo.SaveEventWithOutbox(ctx, event, "canonical-events"); err != nil {
		t.Fatalf("SaveEventWithOutbox failed: %v", err)
	}

	pending, err := repo.FetchPendingMessages(ctx, 100)
	if err != nil {
		t.Fatalf("FetchPendingMessages failed: %v", err)
	}

	var targetID int64
	for _, msg := range pending {
		if msg.EventID == event.EventId {
			targetID = msg.ID
			break
		}
	}

	if targetID == 0 {
		t.Fatal("Target message not found in pending")
	}

	claimed, err := repo.MarkAsProcessing(ctx, []int64{targetID})
	if err != nil {
		t.Fatalf("MarkAsProcessing failed: %v", err)
	}
	if len(claimed) != 1 || claimed[0] != targetID {
		t.Errorf("Expected to claim [%d], got %v", targetID, claimed)
	}

	if err := repo.MarkAsPublished(ctx, []int64{targetID}); err != nil {
		t.Fatalf("MarkAsPublished failed: %v", err)
	}

	pending, err = repo.FetchPendingMessages(ctx, 100)
	if err != nil {
		t.Fatalf("FetchPendingMessages failed: %v", err)
	}

	for _, msg := range pending {
		if msg.ID == targetID {
			t.Error("Message should not be in pending after publish")
		}
	}
}
