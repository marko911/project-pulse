package storage

import (
	"time"
)

// OutboxStatus represents the processing state of an outbox message.
type OutboxStatus string

const (
	OutboxStatusPending    OutboxStatus = "pending"
	OutboxStatusProcessing OutboxStatus = "processing"
	OutboxStatusPublished  OutboxStatus = "published"
	OutboxStatusFailed     OutboxStatus = "failed"
)

// EventRecord represents a canonical event stored in the database.
type EventRecord struct {
	EventID         string    `db:"event_id"`
	Chain           int16     `db:"chain"`
	BlockNumber     int64     `db:"block_number"`
	BlockHash       string    `db:"block_hash"`
	BlockTimestamp  time.Time `db:"block_timestamp"`
	TxHash          string    `db:"tx_hash"`
	TxIndex         int32     `db:"tx_index"`
	EventIndex      int32     `db:"event_index"`
	EventType       string    `db:"event_type"`
	ProgramID       *string   `db:"program_id"`
	Accounts        []byte    `db:"accounts"` // JSONB
	Payload         []byte    `db:"payload"`
	CommitmentLevel int16     `db:"commitment_level"`
	ReorgAction     int16     `db:"reorg_action"`
	ReplacesEventID *string   `db:"replaces_event_id"`
	NativeValue     int64     `db:"native_value"`
	SchemaVersion   int32     `db:"schema_version"`
	IngestedAt      time.Time `db:"ingested_at"`
	CreatedAt       time.Time `db:"created_at"`
}

// OutboxMessage represents a message in the transactional outbox.
type OutboxMessage struct {
	ID           int64        `db:"id"`
	EventID      string       `db:"event_id"`
	Topic        string       `db:"topic"`
	PartitionKey string       `db:"partition_key"`
	Payload      []byte       `db:"payload"`
	Chain        int16        `db:"chain"`
	EventType    string       `db:"event_type"`
	Status       OutboxStatus `db:"status"`
	RetryCount   int32        `db:"retry_count"`
	MaxRetries   int32        `db:"max_retries"`
	LastError    *string      `db:"last_error"`
	CreatedAt    time.Time    `db:"created_at"`
	ProcessedAt  *time.Time   `db:"processed_at"`
	PublishedAt  *time.Time   `db:"published_at"`
}

// EventWithOutbox bundles an event with its outbox message for atomic writes.
type EventWithOutbox struct {
	Event  EventRecord
	Outbox OutboxMessage
}
