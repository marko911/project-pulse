package storage

import (
	"time"
)

type OutboxStatus string

const (
	OutboxStatusPending    OutboxStatus = "pending"
	OutboxStatusProcessing OutboxStatus = "processing"
	OutboxStatusPublished  OutboxStatus = "published"
	OutboxStatusFailed     OutboxStatus = "failed"
)

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
	Accounts        []byte    `db:"accounts"`
	Payload         []byte    `db:"payload"`
	CommitmentLevel int16     `db:"commitment_level"`
	ReorgAction     int16     `db:"reorg_action"`
	ReplacesEventID *string   `db:"replaces_event_id"`
	NativeValue     int64     `db:"native_value"`
	SchemaVersion   int32     `db:"schema_version"`
	IngestedAt      time.Time `db:"ingested_at"`
	CreatedAt       time.Time `db:"created_at"`
}

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

type EventWithOutbox struct {
	Event  EventRecord
	Outbox OutboxMessage
}

type FunctionStatus string

const (
	FunctionStatusActive   FunctionStatus = "active"
	FunctionStatusDisabled FunctionStatus = "disabled"
	FunctionStatusDeleted  FunctionStatus = "deleted"
)

type Function struct {
	ID             string         `db:"id"`
	TenantID       string         `db:"tenant_id"`
	Name           string         `db:"name"`
	Description    *string        `db:"description"`
	RuntimeVersion string         `db:"runtime_version"`
	MaxMemoryMB    int            `db:"max_memory_mb"`
	MaxCPUMs       int            `db:"max_cpu_ms"`
	CurrentVersion int            `db:"current_version"`
	ModuleHash     *string        `db:"module_hash"`
	ModuleSize     *int           `db:"module_size"`
	Status         FunctionStatus `db:"status"`
	CreatedAt      time.Time      `db:"created_at"`
	UpdatedAt      time.Time      `db:"updated_at"`
}

type TriggerStatus string

const (
	TriggerStatusActive   TriggerStatus = "active"
	TriggerStatusDisabled TriggerStatus = "disabled"
	TriggerStatusDeleted  TriggerStatus = "deleted"
)

type Trigger struct {
	ID            string        `db:"id"`
	FunctionID    string        `db:"function_id"`
	TenantID      string        `db:"tenant_id"`
	Name          string        `db:"name"`
	EventType     string        `db:"event_type"`
	FilterChain   *int16        `db:"filter_chain"`
	FilterAddress *string       `db:"filter_address"`
	FilterTopic   *string       `db:"filter_topic"`
	FilterJSON    []byte        `db:"filter_json"`
	Priority      int           `db:"priority"`
	MaxRetries    int           `db:"max_retries"`
	TimeoutMs     int           `db:"timeout_ms"`
	Status        TriggerStatus `db:"status"`
	CreatedAt     time.Time     `db:"created_at"`
	UpdatedAt     time.Time     `db:"updated_at"`
}

type DeploymentStatus string

const (
	DeploymentStatusActive     DeploymentStatus = "active"
	DeploymentStatusSuperseded DeploymentStatus = "superseded"
	DeploymentStatusRollback   DeploymentStatus = "rollback"
	DeploymentStatusFailed     DeploymentStatus = "failed"
)

type Deployment struct {
	ID             string           `db:"id"`
	FunctionID     string           `db:"function_id"`
	TenantID       string           `db:"tenant_id"`
	Version        int              `db:"version"`
	ModuleHash     string           `db:"module_hash"`
	ModuleSize     int              `db:"module_size"`
	ModulePath     string           `db:"module_path"`
	SourceHash     *string          `db:"source_hash"`
	BuildLog       *string          `db:"build_log"`
	DeployedBy     *string          `db:"deployed_by"`
	DeploymentNote *string          `db:"deployment_note"`
	Status         DeploymentStatus `db:"status"`
	CreatedAt      time.Time        `db:"created_at"`
	ActivatedAt    *time.Time       `db:"activated_at"`
}

type Invocation struct {
	ID           string    `db:"id"`
	FunctionID   string    `db:"function_id"`
	TriggerID    *string   `db:"trigger_id"`
	TenantID     string    `db:"tenant_id"`
	DeploymentID string    `db:"deployment_id"`
	RequestID    string    `db:"request_id"`
	InputHash    *string   `db:"input_hash"`
	InputSize    *int      `db:"input_size"`
	Success      bool      `db:"success"`
	OutputHash   *string   `db:"output_hash"`
	OutputSize   *int      `db:"output_size"`
	ErrorMessage *string   `db:"error_message"`
	DurationMs   int       `db:"duration_ms"`
	MemoryBytes  *int64    `db:"memory_bytes"`
	CPUTimeMs    *int      `db:"cpu_time_ms"`
	StartedAt    time.Time `db:"started_at"`
	CompletedAt  time.Time `db:"completed_at"`
}
