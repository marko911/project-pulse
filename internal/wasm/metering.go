package wasm

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type BillingEvent struct {
	EventID     string    `json:"event_id"`
	TenantID    string    `json:"tenant_id"`
	FunctionID  string    `json:"function_id"`
	RequestID   string    `json:"request_id"`
	Timestamp   time.Time `json:"timestamp"`
	DurationMs  int64     `json:"duration_ms"`
	MemoryBytes int64     `json:"memory_bytes"`
	Success     bool      `json:"success"`
	ErrorCode   string    `json:"error_code,omitempty"`
}

type MeteringConfig struct {
	BrokerEndpoint string
	BillingTopic   string
}

type MeteringPublisher struct {
	cfg    MeteringConfig
	client *kgo.Client
	logger *slog.Logger
}

func NewMeteringPublisher(cfg MeteringConfig, logger *slog.Logger) (*MeteringPublisher, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.BrokerEndpoint),
		kgo.DefaultProduceTopic(cfg.BillingTopic),
	)
	if err != nil {
		return nil, err
	}

	return &MeteringPublisher{
		cfg:    cfg,
		client: client,
		logger: logger,
	}, nil
}

func (m *MeteringPublisher) Publish(ctx context.Context, result *InvocationResult) error {
	event := BillingEvent{
		EventID:     result.RequestID + "-billing",
		TenantID:    result.TenantID,
		FunctionID:  result.FunctionID,
		RequestID:   result.RequestID,
		Timestamp:   time.Now().UTC(),
		DurationMs:  result.DurationMs,
		MemoryBytes: result.MemoryBytes,
		Success:     result.Success,
	}

	if !result.Success && result.Error != "" {
		event.ErrorCode = "EXECUTION_ERROR"
		if len(result.Error) > 50 {
			event.ErrorCode = result.Error[:50]
		}
	}

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	record := &kgo.Record{
		Key:   []byte(result.TenantID),
		Value: data,
	}

	m.client.Produce(ctx, record, func(r *kgo.Record, err error) {
		if err != nil {
			m.logger.Error("failed to publish billing event",
				"tenant_id", result.TenantID,
				"request_id", result.RequestID,
				"error", err,
			)
		} else {
			m.logger.Debug("published billing event",
				"tenant_id", result.TenantID,
				"request_id", result.RequestID,
				"duration_ms", result.DurationMs,
				"memory_bytes", result.MemoryBytes,
				"partition", r.Partition,
				"offset", r.Offset,
			)
		}
	})

	return nil
}

func (m *MeteringPublisher) PublishSync(ctx context.Context, result *InvocationResult) error {
	event := BillingEvent{
		EventID:     result.RequestID + "-billing",
		TenantID:    result.TenantID,
		FunctionID:  result.FunctionID,
		RequestID:   result.RequestID,
		Timestamp:   time.Now().UTC(),
		DurationMs:  result.DurationMs,
		MemoryBytes: result.MemoryBytes,
		Success:     result.Success,
	}

	if !result.Success && result.Error != "" {
		event.ErrorCode = "EXECUTION_ERROR"
	}

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	record := &kgo.Record{
		Key:   []byte(result.TenantID),
		Value: data,
	}

	results := m.client.ProduceSync(ctx, record)
	return results.FirstErr()
}

func (m *MeteringPublisher) Flush(ctx context.Context) error {
	return m.client.Flush(ctx)
}

func (m *MeteringPublisher) Close() {
	m.client.Close()
}
