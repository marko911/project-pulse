// Package backfill implements the backfill orchestration for gap recovery.
package backfill

import (
	"time"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

// BackfillStatus represents the current state of a backfill request.
type BackfillStatus string

const (
	// BackfillStatusPending indicates the backfill request has been created but not yet processed.
	BackfillStatusPending BackfillStatus = "pending"

	// BackfillStatusInProgress indicates the backfill is actively being processed.
	BackfillStatusInProgress BackfillStatus = "in_progress"

	// BackfillStatusCompleted indicates the backfill completed successfully.
	BackfillStatusCompleted BackfillStatus = "completed"

	// BackfillStatusFailed indicates the backfill failed.
	BackfillStatusFailed BackfillStatus = "failed"

	// BackfillStatusRetrying indicates the backfill is being retried.
	BackfillStatusRetrying BackfillStatus = "retrying"
)

// BackfillPriority indicates urgency of backfill request.
type BackfillPriority int

const (
	// BackfillPriorityNormal is default priority for detected gaps.
	BackfillPriorityNormal BackfillPriority = 0

	// BackfillPriorityHigh is for gaps that block downstream processing.
	BackfillPriorityHigh BackfillPriority = 1

	// BackfillPriorityCritical is for gaps requiring immediate attention.
	BackfillPriorityCritical BackfillPriority = 2
)

// BackfillRequest represents a request to re-ingest a range of blocks.
// This is published to the backfill-requests topic for adapters to consume.
type BackfillRequest struct {
	// RequestID is a unique identifier for this backfill request.
	RequestID string `json:"request_id"`

	// Chain is the blockchain to backfill.
	Chain protov1.Chain `json:"chain"`

	// CommitmentLevel is the commitment level to backfill at.
	CommitmentLevel protov1.CommitmentLevel `json:"commitment_level"`

	// StartBlock is the first block to backfill (inclusive).
	StartBlock uint64 `json:"start_block"`

	// EndBlock is the last block to backfill (inclusive).
	EndBlock uint64 `json:"end_block"`

	// Priority indicates urgency of this backfill.
	Priority BackfillPriority `json:"priority"`

	// SourceGapID is the ID of the gap event that triggered this request (for tracking).
	SourceGapID string `json:"source_gap_id,omitempty"`

	// RequestedAt is when the backfill was requested.
	RequestedAt time.Time `json:"requested_at"`

	// RetryCount tracks how many times this request has been retried.
	RetryCount int `json:"retry_count"`

	// MaxRetries is the maximum number of retries before marking as failed.
	MaxRetries int `json:"max_retries"`
}

// GapEvent represents a gap detected by the gap detector.
// This matches the format published by the gap-detector service.
type GapEvent struct {
	// EventID is a unique identifier for this gap event.
	EventID string `json:"event_id"`

	// Chain is the blockchain where the gap was detected.
	Chain protov1.Chain `json:"chain"`

	// CommitmentLevel is the commitment level where the gap occurred.
	CommitmentLevel int16 `json:"commitment_level"`

	// ExpectedBlock is the block number that was expected.
	ExpectedBlock uint64 `json:"expected_block"`

	// ReceivedBlock is the block number that was actually received.
	ReceivedBlock uint64 `json:"received_block"`

	// GapSize is the number of missing blocks.
	GapSize uint64 `json:"gap_size"`

	// DetectedAt is when the gap was detected.
	DetectedAt time.Time `json:"detected_at"`

	// Topic is the source topic where the gap was detected.
	Topic string `json:"topic"`
}

// BackfillResult represents the outcome of a backfill operation.
type BackfillResult struct {
	// RequestID matches the original BackfillRequest.
	RequestID string `json:"request_id"`

	// Status is the final status of the backfill.
	Status BackfillStatus `json:"status"`

	// BlocksProcessed is how many blocks were successfully backfilled.
	BlocksProcessed uint64 `json:"blocks_processed"`

	// Error contains any error message if the backfill failed.
	Error string `json:"error,omitempty"`

	// CompletedAt is when the backfill completed (success or failure).
	CompletedAt time.Time `json:"completed_at"`

	// Duration is how long the backfill took.
	Duration time.Duration `json:"duration"`
}

// BackfillTracker tracks the state of pending backfill requests.
type BackfillTracker struct {
	// RequestID is the unique request identifier.
	RequestID string

	// Request is the original backfill request.
	Request *BackfillRequest

	// Status is the current status.
	Status BackfillStatus

	// StartedAt is when processing began.
	StartedAt time.Time

	// LastUpdated is the last status update time.
	LastUpdated time.Time

	// ProgressBlocks tracks how many blocks have been processed so far.
	ProgressBlocks uint64
}
