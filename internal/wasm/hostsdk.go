package wasm

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/redis/go-redis/v9"
)

type HostSDKConfig struct {
	RedisAddr string
}

type HostSDK struct {
	cfg    HostSDKConfig
	redis  *redis.Client
	logger *slog.Logger
}

func NewHostSDK(cfg HostSDKConfig, logger *slog.Logger) (*HostSDK, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		logger.Warn("failed to connect to Redis, KV operations will fail", "error", err)
	}

	return &HostSDK{
		cfg:    cfg,
		redis:  rdb,
		logger: logger,
	}, nil
}

func (h *HostSDK) GetHostFunctions(ctx context.Context, tenantID string) *HostFunctions {
	return &HostFunctions{
		tenantID: tenantID,
		sdk:      h,
		logger:   h.logger.With("tenant_id", tenantID),
		ctx:      ctx,
	}
}

func (h *HostSDK) Close() error {
	return h.redis.Close()
}

type HostFunctions struct {
	tenantID string
	sdk      *HostSDK
	logger   *slog.Logger
	ctx      context.Context

	mu     sync.Mutex
	input  []byte
	output []byte
	logs   []LogEntry
}

type LogEntry struct {
	Level   int
	Message string
}

const (
	LogLevelDebug = 0
	LogLevelInfo  = 1
	LogLevelWarn  = 2
	LogLevelError = 3
)

func (h *HostFunctions) Log(level int, message string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.logs = append(h.logs, LogEntry{Level: level, Message: message})

	switch level {
	case LogLevelDebug:
		h.logger.Debug(message, "source", "wasm")
	case LogLevelInfo:
		h.logger.Info(message, "source", "wasm")
	case LogLevelWarn:
		h.logger.Warn(message, "source", "wasm")
	case LogLevelError:
		h.logger.Error(message, "source", "wasm")
	default:
		h.logger.Info(message, "source", "wasm", "level", level)
	}
}

func (h *HostFunctions) SetInput(data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.input = make([]byte, len(data))
	copy(h.input, data)
}

func (h *HostFunctions) GetInput() []byte {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.input
}

func (h *HostFunctions) GetInputLen() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return len(h.input)
}

func (h *HostFunctions) SetOutput(data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.output = make([]byte, len(data))
	copy(h.output, data)
}

func (h *HostFunctions) GetOutput() []byte {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.output
}

func (h *HostFunctions) GetLogs() []LogEntry {
	h.mu.Lock()
	defer h.mu.Unlock()

	logs := make([]LogEntry, len(h.logs))
	copy(logs, h.logs)
	return logs
}

func (h *HostFunctions) KVGet(ctx context.Context, key string) ([]byte, error) {
	redisKey := h.tenantKey(key)
	val, err := h.sdk.redis.Get(ctx, redisKey).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (h *HostFunctions) KVSet(ctx context.Context, key string, value []byte) error {
	redisKey := h.tenantKey(key)
	return h.sdk.redis.Set(ctx, redisKey, value, 0).Err()
}

func (h *HostFunctions) KVDelete(ctx context.Context, key string) error {
	redisKey := h.tenantKey(key)
	return h.sdk.redis.Del(ctx, redisKey).Err()
}

func (h *HostFunctions) KVList(ctx context.Context, prefix string) ([]string, error) {
	pattern := h.tenantKey(prefix) + "*"
	keys, err := h.sdk.redis.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, err
	}

	prefixLen := len(h.tenantKey(""))
	result := make([]string, len(keys))
	for i, k := range keys {
		if len(k) > prefixLen {
			result[i] = k[prefixLen:]
		}
	}
	return result, nil
}

func (h *HostFunctions) tenantKey(key string) string {
	return fmt.Sprintf("tenant:%s:kv:%s", h.tenantID, key)
}
