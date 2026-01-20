package backfill

import (
	"context"
	"testing"
	"time"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

func TestNewOrchestrator(t *testing.T) {
	cfg := DefaultOrchestratorConfig()
	cfg.Brokers = []string{"localhost:9092"}

	orch, err := NewOrchestrator(cfg, nil)
	if err != nil {
		t.Fatalf("failed to create orchestrator: %v", err)
	}

	if orch == nil {
		t.Fatal("orchestrator should not be nil")
	}

	if orch.pending == nil {
		t.Error("pending map should be initialized")
	}
}

func TestNewOrchestrator_NoBrokers(t *testing.T) {
	cfg := DefaultOrchestratorConfig()
	cfg.Brokers = nil

	_, err := NewOrchestrator(cfg, nil)
	if err == nil {
		t.Error("expected error when no brokers configured")
	}
}

func TestDefaultOrchestratorConfig(t *testing.T) {
	cfg := DefaultOrchestratorConfig()

	if cfg.GapEventsTopic != "gap-events" {
		t.Errorf("expected gap-events topic, got %s", cfg.GapEventsTopic)
	}
	if cfg.BackfillRequestsTopic != "backfill-requests" {
		t.Errorf("expected backfill-requests topic, got %s", cfg.BackfillRequestsTopic)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("expected max retries 3, got %d", cfg.MaxRetries)
	}
	if cfg.DefaultPriority != BackfillPriorityNormal {
		t.Errorf("expected normal priority, got %d", cfg.DefaultPriority)
	}
}

func TestOrchestrator_CreateBackfillRequest(t *testing.T) {
	cfg := DefaultOrchestratorConfig()
	cfg.Brokers = []string{"localhost:9092"}
	cfg.MaxRetries = 5

	orch, _ := NewOrchestrator(cfg, nil)

	gap := &GapEvent{
		EventID:         "gap-123",
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: 3,
		ExpectedBlock:   100,
		ReceivedBlock:   105,
		GapSize:         5,
		DetectedAt:      time.Now(),
		Topic:           "finalized-events",
	}

	request := orch.createBackfillRequest(gap)

	if request == nil {
		t.Fatal("request should not be nil")
	}

	if request.RequestID == "" {
		t.Error("request ID should be generated")
	}

	if request.Chain != protov1.Chain_CHAIN_ETHEREUM {
		t.Errorf("expected ethereum chain, got %d", request.Chain)
	}

	if request.StartBlock != 100 {
		t.Errorf("expected start block 100, got %d", request.StartBlock)
	}

	if request.EndBlock != 104 {
		t.Errorf("expected end block 104, got %d", request.EndBlock)
	}

	if request.SourceGapID != "gap-123" {
		t.Errorf("expected source gap ID gap-123, got %s", request.SourceGapID)
	}

	if request.MaxRetries != 5 {
		t.Errorf("expected max retries 5, got %d", request.MaxRetries)
	}

	if request.RetryCount != 0 {
		t.Errorf("expected retry count 0, got %d", request.RetryCount)
	}
}

func TestOrchestrator_TrackRequest(t *testing.T) {
	cfg := DefaultOrchestratorConfig()
	cfg.Brokers = []string{"localhost:9092"}

	orch, _ := NewOrchestrator(cfg, nil)

	request := &BackfillRequest{
		RequestID:  "req-123",
		Chain:      protov1.Chain_CHAIN_ETHEREUM,
		StartBlock: 100,
		EndBlock:   104,
	}

	orch.trackRequest(request)

	if orch.GetPendingCount() != 1 {
		t.Errorf("expected 1 pending, got %d", orch.GetPendingCount())
	}

	if orch.GetActiveCount() != 1 {
		t.Errorf("expected 1 active, got %d", orch.GetActiveCount())
	}

	tracker, exists := orch.pending[request.RequestID]
	if !exists {
		t.Fatal("tracker should exist")
	}

	if tracker.Status != BackfillStatusPending {
		t.Errorf("expected pending status, got %s", tracker.Status)
	}
}

func TestOrchestrator_GetStats(t *testing.T) {
	cfg := DefaultOrchestratorConfig()
	cfg.Brokers = []string{"localhost:9092"}

	orch, _ := NewOrchestrator(cfg, nil)

	// Simulate some activity
	orch.mu.Lock()
	orch.stats.GapsReceived = 10
	orch.stats.BackfillsCreated = 10
	orch.stats.BackfillsComplete = 8
	orch.stats.BackfillsFailed = 1
	orch.stats.TotalBlocksFilled = 1000
	orch.mu.Unlock()

	stats := orch.GetStats()

	if stats.GapsReceived != 10 {
		t.Errorf("expected 10 gaps received, got %d", stats.GapsReceived)
	}
	if stats.BackfillsCreated != 10 {
		t.Errorf("expected 10 backfills created, got %d", stats.BackfillsCreated)
	}
	if stats.BackfillsComplete != 8 {
		t.Errorf("expected 8 complete, got %d", stats.BackfillsComplete)
	}
	if stats.BackfillsFailed != 1 {
		t.Errorf("expected 1 failed, got %d", stats.BackfillsFailed)
	}
	if stats.TotalBlocksFilled != 1000 {
		t.Errorf("expected 1000 blocks filled, got %d", stats.TotalBlocksFilled)
	}
}

func TestOrchestrator_Shutdown(t *testing.T) {
	cfg := DefaultOrchestratorConfig()
	cfg.Brokers = []string{"localhost:9092"}

	orch, _ := NewOrchestrator(cfg, nil)

	ctx := context.Background()
	err := orch.Shutdown(ctx)
	if err != nil {
		t.Errorf("shutdown should not error: %v", err)
	}

	orch.mu.RLock()
	if !orch.shutdown {
		t.Error("shutdown flag should be set")
	}
	orch.mu.RUnlock()
}

func TestBackfillRequest_BlockRange(t *testing.T) {
	request := &BackfillRequest{
		StartBlock: 100,
		EndBlock:   109,
	}

	blockCount := request.EndBlock - request.StartBlock + 1
	if blockCount != 10 {
		t.Errorf("expected 10 blocks in range, got %d", blockCount)
	}
}

func TestBackfillStatus_Values(t *testing.T) {
	statuses := []BackfillStatus{
		BackfillStatusPending,
		BackfillStatusInProgress,
		BackfillStatusCompleted,
		BackfillStatusFailed,
		BackfillStatusRetrying,
	}

	for _, status := range statuses {
		if status == "" {
			t.Error("status should not be empty string")
		}
	}
}

func TestBackfillPriority_Values(t *testing.T) {
	if BackfillPriorityNormal >= BackfillPriorityHigh {
		t.Error("normal priority should be less than high")
	}
	if BackfillPriorityHigh >= BackfillPriorityCritical {
		t.Error("high priority should be less than critical")
	}
}

func TestGapEvent_JSON(t *testing.T) {
	gap := GapEvent{
		EventID:         "gap-123",
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		CommitmentLevel: 3,
		ExpectedBlock:   100,
		ReceivedBlock:   105,
		GapSize:         5,
		DetectedAt:      time.Now(),
		Topic:           "finalized-events",
	}

	if gap.EventID != "gap-123" {
		t.Errorf("expected gap-123, got %s", gap.EventID)
	}
	if gap.GapSize != 5 {
		t.Errorf("expected gap size 5, got %d", gap.GapSize)
	}
}

func TestBackfillResult_Success(t *testing.T) {
	result := BackfillResult{
		RequestID:       "req-123",
		Status:          BackfillStatusCompleted,
		BlocksProcessed: 10,
		CompletedAt:     time.Now(),
		Duration:        5 * time.Second,
	}

	if result.Status != BackfillStatusCompleted {
		t.Errorf("expected completed status, got %s", result.Status)
	}
	if result.Error != "" {
		t.Error("successful result should not have error")
	}
}

func TestBackfillResult_Failure(t *testing.T) {
	result := BackfillResult{
		RequestID:       "req-123",
		Status:          BackfillStatusFailed,
		BlocksProcessed: 5,
		Error:           "connection timeout",
		CompletedAt:     time.Now(),
		Duration:        30 * time.Second,
	}

	if result.Status != BackfillStatusFailed {
		t.Errorf("expected failed status, got %s", result.Status)
	}
	if result.Error == "" {
		t.Error("failed result should have error message")
	}
}

func TestBackfillTracker_Creation(t *testing.T) {
	request := &BackfillRequest{
		RequestID:  "req-123",
		StartBlock: 100,
		EndBlock:   109,
	}

	tracker := &BackfillTracker{
		RequestID:   request.RequestID,
		Request:     request,
		Status:      BackfillStatusPending,
		StartedAt:   time.Now(),
		LastUpdated: time.Now(),
	}

	if tracker.RequestID != "req-123" {
		t.Errorf("expected req-123, got %s", tracker.RequestID)
	}
	if tracker.ProgressBlocks != 0 {
		t.Errorf("expected 0 progress, got %d", tracker.ProgressBlocks)
	}
}

func BenchmarkCreateBackfillRequest(b *testing.B) {
	cfg := DefaultOrchestratorConfig()
	cfg.Brokers = []string{"localhost:9092"}

	orch, _ := NewOrchestrator(cfg, nil)

	gap := &GapEvent{
		Chain:         protov1.Chain_CHAIN_ETHEREUM,
		ExpectedBlock: 100,
		ReceivedBlock: 105,
		GapSize:       5,
		DetectedAt:    time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		orch.createBackfillRequest(gap)
	}
}
