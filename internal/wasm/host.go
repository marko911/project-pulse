// Package wasm provides the WASM function execution runtime using wasmtime.
package wasm

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// HostConfig contains configuration for the WASM host.
type HostConfig struct {
	// Kafka/Redpanda
	BrokerEndpoint  string
	InvocationTopic string
	ResultTopic     string
	BillingTopic    string
	ConsumerGroup   string

	// S3/MinIO
	S3Endpoint  string
	S3Bucket    string
	S3AccessKey string
	S3SecretKey string
	S3UseSSL    bool

	// Redis
	RedisAddr string

	// Runtime
	WorkerCount     int
	MaxMemoryMB     int
	MaxCPUMs        int
	ModuleCacheSize int
}

// InvocationRequest represents a function invocation request.
// Matches the FunctionInvocation struct from trigger-router.
type InvocationRequest struct {
	InvocationID string                 `json:"invocation_id"`
	FunctionID   string                 `json:"function_id"`
	TriggerID    string                 `json:"trigger_id"`
	TenantID     string                 `json:"tenant_id"`
	Event        map[string]interface{} `json:"event"`
	CreatedAt    string                 `json:"created_at"`
}

// InvocationResult represents the result of a function execution.
type InvocationResult struct {
	RequestID   string `json:"request_id"`
	FunctionID  string `json:"function_id"`
	TenantID    string `json:"tenant_id"`
	Success     bool   `json:"success"`
	Output      []byte `json:"output"`
	Error       string `json:"error,omitempty"`
	DurationMs  int64  `json:"duration_ms"`
	MemoryBytes int64  `json:"memory_bytes"`
}

// Host manages WASM function execution.
type Host struct {
	cfg      HostConfig
	logger   *slog.Logger
	runtime  *Runtime
	loader   *ModuleLoader
	sdk      *HostSDK
	metering *MeteringPublisher
	consumer *kgo.Client
	producer *kgo.Client
	workers  []*Worker
	wg       sync.WaitGroup
}

// NewHost creates a new WASM host instance.
func NewHost(cfg HostConfig, logger *slog.Logger) (*Host, error) {
	// Create runtime with wasmtime engine
	runtime, err := NewRuntime(RuntimeConfig{
		MaxMemoryMB: cfg.MaxMemoryMB,
		MaxCPUMs:    cfg.MaxCPUMs,
		CacheSize:   cfg.ModuleCacheSize,
	}, logger)
	if err != nil {
		return nil, err
	}

	// Create module loader for S3/MinIO
	loader, err := NewModuleLoader(LoaderConfig{
		Endpoint:  cfg.S3Endpoint,
		Bucket:    cfg.S3Bucket,
		AccessKey: cfg.S3AccessKey,
		SecretKey: cfg.S3SecretKey,
		UseSSL:    cfg.S3UseSSL,
	}, logger)
	if err != nil {
		return nil, err
	}
	loader.SetRuntime(runtime)

	// Create host SDK for WASM functions
	sdk, err := NewHostSDK(HostSDKConfig{
		RedisAddr: cfg.RedisAddr,
	}, logger)
	if err != nil {
		return nil, err
	}

	// Create metering publisher for billing events
	metering, err := NewMeteringPublisher(MeteringConfig{
		BrokerEndpoint: cfg.BrokerEndpoint,
		BillingTopic:   cfg.BillingTopic,
	}, logger)
	if err != nil {
		return nil, err
	}

	// Create Kafka consumer for function-invocations topic
	brokerList := strings.Split(cfg.BrokerEndpoint, ",")
	for i := range brokerList {
		brokerList[i] = strings.TrimSpace(brokerList[i])
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokerList...),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.ConsumeTopics(cfg.InvocationTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}

	// Create Kafka producer for function-results topic
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokerList...),
		kgo.MaxProduceRequestsInflightPerBroker(1),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.RecordRetries(5),
	)
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}

	logger.Info("connected to kafka",
		"brokers", brokerList,
		"invocation_topic", cfg.InvocationTopic,
		"result_topic", cfg.ResultTopic,
		"consumer_group", cfg.ConsumerGroup,
	)

	return &Host{
		cfg:      cfg,
		logger:   logger,
		runtime:  runtime,
		loader:   loader,
		sdk:      sdk,
		metering: metering,
		consumer: consumer,
		producer: producer,
	}, nil
}

// Run starts the WASM host and blocks until context is cancelled.
func (h *Host) Run(ctx context.Context) error {
	h.logger.Info("starting wasm host workers", "count", h.cfg.WorkerCount)

	// Create worker pool
	invocations := make(chan InvocationRequest, h.cfg.WorkerCount*10)
	results := make(chan InvocationResult, h.cfg.WorkerCount*10)

	// Start workers
	for i := 0; i < h.cfg.WorkerCount; i++ {
		worker := NewWorker(i, h.runtime, h.loader, h.sdk, h.logger)
		h.workers = append(h.workers, worker)

		h.wg.Add(1)
		go func(w *Worker) {
			defer h.wg.Done()
			w.Run(ctx, invocations, results)
		}(worker)
	}

	// Start result publisher
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.publishResults(ctx, results)
	}()

	// Start invocation consumer
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.consumeInvocations(ctx, invocations)
	}()

	// Wait for shutdown
	<-ctx.Done()
	h.logger.Info("shutting down wasm host")

	// Close channels to signal workers
	close(invocations)

	// Wait for all workers to finish
	h.wg.Wait()
	close(results)

	return nil
}

// consumeInvocations consumes invocation requests from Kafka.
func (h *Host) consumeInvocations(ctx context.Context, out chan<- InvocationRequest) {
	h.logger.Info("starting invocation consumer",
		"topic", h.cfg.InvocationTopic,
		"group", h.cfg.ConsumerGroup,
	)

	for {
		select {
		case <-ctx.Done():
			h.logger.Info("invocation consumer shutting down")
			// Final commit before exit
			if err := h.consumer.CommitUncommittedOffsets(context.Background()); err != nil {
				h.logger.Error("final commit error", "error", err)
			}
			h.consumer.Close()
			return
		default:
		}

		fetches := h.consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == context.Canceled {
					continue
				}
				h.logger.Error("fetch error",
					"topic", e.Topic,
					"partition", e.Partition,
					"error", e.Err,
				)
			}
			continue
		}

		fetches.EachRecord(func(record *kgo.Record) {
			var req InvocationRequest
			if err := json.Unmarshal(record.Value, &req); err != nil {
				h.logger.Error("failed to unmarshal invocation request",
					"offset", record.Offset,
					"error", err,
				)
				return
			}

			h.logger.Debug("received invocation request",
				"request_id", req.InvocationID,
				"function_id", req.FunctionID,
				"tenant_id", req.TenantID,
			)

			select {
			case out <- req:
			case <-ctx.Done():
				return
			}
		})

		// Commit offsets after processing batch
		if err := h.consumer.CommitUncommittedOffsets(ctx); err != nil && err != context.Canceled {
			h.logger.Error("commit error", "error", err)
		}
	}
}

// publishResults publishes execution results to Kafka.
func (h *Host) publishResults(ctx context.Context, in <-chan InvocationResult) {
	h.logger.Info("starting result publisher", "topic", h.cfg.ResultTopic)

	defer func() {
		// Flush and close producer on shutdown
		if err := h.producer.Flush(context.Background()); err != nil {
			h.logger.Error("producer flush error", "error", err)
		}
		h.producer.Close()
	}()

	for {
		select {
		case result, ok := <-in:
			if !ok {
				return
			}

			// Publish to Kafka
			if err := h.publishResult(ctx, &result); err != nil {
				h.logger.Error("failed to publish result to kafka",
					"request_id", result.RequestID,
					"error", err,
				)
			} else {
				h.logger.Debug("published result",
					"request_id", result.RequestID,
					"success", result.Success,
					"duration_ms", result.DurationMs,
				)
			}

			// Emit billing event for metering
			if err := h.metering.Publish(ctx, &result); err != nil {
				h.logger.Error("failed to publish billing event",
					"request_id", result.RequestID,
					"error", err,
				)
			}
		case <-ctx.Done():
			// Drain remaining results
			for result := range in {
				if err := h.publishResult(context.Background(), &result); err != nil {
					h.logger.Error("failed to publish final result", "request_id", result.RequestID, "error", err)
				}
			}
			return
		}
	}
}

// publishResult publishes a single result to the function-results Kafka topic.
func (h *Host) publishResult(ctx context.Context, result *InvocationResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}

	record := &kgo.Record{
		Topic: h.cfg.ResultTopic,
		Key:   []byte(result.FunctionID), // Partition by function for ordering
		Value: data,
		Headers: []kgo.RecordHeader{
			{Key: "request_id", Value: []byte(result.RequestID)},
			{Key: "function_id", Value: []byte(result.FunctionID)},
			{Key: "tenant_id", Value: []byte(result.TenantID)},
			{Key: "success", Value: []byte(fmt.Sprintf("%t", result.Success))},
		},
	}

	results := h.producer.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("produce: %w", err)
	}

	return nil
}

// Worker executes WASM functions.
type Worker struct {
	id      int
	runtime *Runtime
	loader  *ModuleLoader
	sdk     *HostSDK
	logger  *slog.Logger
}

// NewWorker creates a new execution worker.
func NewWorker(id int, runtime *Runtime, loader *ModuleLoader, sdk *HostSDK, logger *slog.Logger) *Worker {
	return &Worker{
		id:      id,
		runtime: runtime,
		loader:  loader,
		sdk:     sdk,
		logger:  logger.With("worker_id", id),
	}
}

// Run processes invocation requests until the channel is closed.
func (w *Worker) Run(ctx context.Context, in <-chan InvocationRequest, out chan<- InvocationResult) {
	w.logger.Info("worker started")

	for {
		select {
		case req, ok := <-in:
			if !ok {
				w.logger.Info("worker stopped")
				return
			}
			result := w.execute(ctx, req)
			out <- result
		case <-ctx.Done():
			w.logger.Info("worker shutting down")
			return
		}
	}
}

// execute runs a single function invocation.
func (w *Worker) execute(ctx context.Context, req InvocationRequest) InvocationResult {
	w.logger.Debug("executing function",
		"request_id", req.InvocationID,
		"function_id", req.FunctionID,
		"tenant_id", req.TenantID,
	)

	// Load the WASM module
	module, err := w.loader.Load(ctx, req.FunctionID)
	if err != nil {
		return InvocationResult{
			RequestID:  req.InvocationID,
			FunctionID: req.FunctionID,
			TenantID:   req.TenantID,
			Success:    false,
			Error:      "failed to load module: " + err.Error(),
		}
	}

	// Marshal the event as payload for the WASM function
	payload, err := json.Marshal(req.Event)
	if err != nil {
		return InvocationResult{
			RequestID:  req.InvocationID,
			FunctionID: req.FunctionID,
			TenantID:   req.TenantID,
			Success:    false,
			Error:      "failed to marshal event: " + err.Error(),
		}
	}

	// Execute the function
	hostFuncs := w.sdk.GetHostFunctions(ctx, req.TenantID)
	result, err := w.runtime.Execute(ctx, module, payload, hostFuncs)
	if err != nil {
		return InvocationResult{
			RequestID:  req.InvocationID,
			FunctionID: req.FunctionID,
			TenantID:   req.TenantID,
			Success:    false,
			Error:      "execution failed: " + err.Error(),
		}
	}

	return InvocationResult{
		RequestID:   req.InvocationID,
		FunctionID:  req.FunctionID,
		TenantID:    req.TenantID,
		Success:     true,
		Output:      result.Output,
		DurationMs:  result.DurationMs,
		MemoryBytes: result.MemoryBytes,
	}
}
