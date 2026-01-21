package backfill

import (
	"time"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

type BackfillStatus string

const (
	BackfillStatusPending BackfillStatus = "pending"

	BackfillStatusInProgress BackfillStatus = "in_progress"

	BackfillStatusCompleted BackfillStatus = "completed"

	BackfillStatusFailed BackfillStatus = "failed"

	BackfillStatusRetrying BackfillStatus = "retrying"
)

type BackfillPriority int

const (
	BackfillPriorityNormal BackfillPriority = 0

	BackfillPriorityHigh BackfillPriority = 1

	BackfillPriorityCritical BackfillPriority = 2
)

type BackfillRequest struct {
	RequestID string `json:"request_id"`

	Chain protov1.Chain `json:"chain"`

	CommitmentLevel protov1.CommitmentLevel `json:"commitment_level"`

	StartBlock uint64 `json:"start_block"`

	EndBlock uint64 `json:"end_block"`

	Priority BackfillPriority `json:"priority"`

	SourceGapID string `json:"source_gap_id,omitempty"`

	RequestedAt time.Time `json:"requested_at"`

	RetryCount int `json:"retry_count"`

	MaxRetries int `json:"max_retries"`
}

type GapEvent struct {
	EventID string `json:"event_id"`

	Chain protov1.Chain `json:"chain"`

	CommitmentLevel int16 `json:"commitment_level"`

	ExpectedBlock uint64 `json:"expected_block"`

	ReceivedBlock uint64 `json:"received_block"`

	GapSize uint64 `json:"gap_size"`

	DetectedAt time.Time `json:"detected_at"`

	Topic string `json:"topic"`
}

type BackfillResult struct {
	RequestID string `json:"request_id"`

	Status BackfillStatus `json:"status"`

	BlocksProcessed uint64 `json:"blocks_processed"`

	Error string `json:"error,omitempty"`

	CompletedAt time.Time `json:"completed_at"`

	Duration time.Duration `json:"duration"`
}

type BackfillTracker struct {
	RequestID string

	Request *BackfillRequest

	Status BackfillStatus

	StartedAt time.Time

	LastUpdated time.Time

	ProgressBlocks uint64
}
