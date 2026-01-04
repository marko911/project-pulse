// Package durable implements Durable Objects - single-threaded actors with persistent state.
//
// Durable Objects provide:
// - Strong consistency: All reads and writes to state are transactional
// - Single-execution: Only one instance of each object runs at a time
// - Persistent state: State survives restarts and is stored in Redis
//
// This is inspired by Cloudflare's Durable Objects but implemented with Redis.
package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis key patterns
const (
	keyDOLock   = "do:lock:"   // do:lock:{namespace}:{id} -> lock holder
	keyDOState  = "do:state:"  // do:state:{namespace}:{id} -> state JSON
	keyDOAlarm  = "do:alarm:"  // do:alarm:{namespace}:{id} -> alarm timestamp
	keyDOTenants = "do:tenants:" // do:tenants:{namespace}:{id} -> tenant ID
)

// Config holds configuration for the Durable Object runtime.
type Config struct {
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	KeyPrefix     string

	// LockTimeout is how long a lock can be held before expiring
	LockTimeout time.Duration

	// LockRetryInterval is how often to retry acquiring a lock
	LockRetryInterval time.Duration

	// MaxLockRetries is the maximum number of lock acquisition attempts
	MaxLockRetries int
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		RedisAddr:         "localhost:6379",
		LockTimeout:       30 * time.Second,
		LockRetryInterval: 100 * time.Millisecond,
		MaxLockRetries:    50,
	}
}

// Runtime manages Durable Object instances.
type Runtime struct {
	cfg    Config
	client *redis.Client

	// Local cache of active objects (for single-node optimization)
	objects sync.Map // objectKey -> *Object
}

// NewRuntime creates a new Durable Object runtime.
func NewRuntime(cfg Config) (*Runtime, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return &Runtime{
		cfg:    cfg,
		client: client,
	}, nil
}

// Get returns or creates a Durable Object instance.
// The object is locked for exclusive access until Close() is called.
func (r *Runtime) Get(ctx context.Context, namespace, id, tenantID string) (*Object, error) {
	key := r.objectKey(namespace, id)

	// Try to acquire the lock
	lockKey := r.lockKey(namespace, id)
	lockValue := generateLockID()

	acquired, err := r.acquireLock(ctx, lockKey, lockValue)
	if err != nil {
		return nil, fmt.Errorf("acquire lock: %w", err)
	}
	if !acquired {
		return nil, ErrObjectLocked
	}

	// Verify tenant access
	existingTenant, err := r.getTenantID(ctx, namespace, id)
	if err != nil {
		r.releaseLock(ctx, lockKey, lockValue)
		return nil, fmt.Errorf("get tenant: %w", err)
	}
	if existingTenant != "" && existingTenant != tenantID {
		r.releaseLock(ctx, lockKey, lockValue)
		return nil, ErrTenantMismatch
	}

	// Set tenant if not set
	if existingTenant == "" {
		if err := r.setTenantID(ctx, namespace, id, tenantID); err != nil {
			r.releaseLock(ctx, lockKey, lockValue)
			return nil, fmt.Errorf("set tenant: %w", err)
		}
	}

	// Load existing state
	state, err := r.loadState(ctx, namespace, id)
	if err != nil {
		r.releaseLock(ctx, lockKey, lockValue)
		return nil, fmt.Errorf("load state: %w", err)
	}

	obj := &Object{
		runtime:   r,
		namespace: namespace,
		id:        id,
		tenantID:  tenantID,
		lockKey:   lockKey,
		lockValue: lockValue,
		state:     state,
	}

	r.objects.Store(key, obj)

	return obj, nil
}

func (r *Runtime) objectKey(namespace, id string) string {
	return fmt.Sprintf("%s:%s", namespace, id)
}

func (r *Runtime) lockKey(namespace, id string) string {
	return r.cfg.KeyPrefix + keyDOLock + namespace + ":" + id
}

func (r *Runtime) stateKey(namespace, id string) string {
	return r.cfg.KeyPrefix + keyDOState + namespace + ":" + id
}

func (r *Runtime) tenantKey(namespace, id string) string {
	return r.cfg.KeyPrefix + keyDOTenants + namespace + ":" + id
}

func (r *Runtime) alarmKey(namespace, id string) string {
	return r.cfg.KeyPrefix + keyDOAlarm + namespace + ":" + id
}

func (r *Runtime) acquireLock(ctx context.Context, key, value string) (bool, error) {
	for i := 0; i < r.cfg.MaxLockRetries; i++ {
		// Try to set the lock with NX (only if not exists)
		ok, err := r.client.SetNX(ctx, key, value, r.cfg.LockTimeout).Result()
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}

		// Lock exists, wait and retry
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(r.cfg.LockRetryInterval):
		}
	}

	return false, nil
}

func (r *Runtime) releaseLock(ctx context.Context, key, value string) error {
	// Only release if we still hold the lock (Lua script for atomicity)
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)

	_, err := script.Run(ctx, r.client, []string{key}, value).Result()
	return err
}

func (r *Runtime) extendLock(ctx context.Context, key, value string) error {
	// Extend lock TTL if we still hold it
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("pexpire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`)

	ttlMs := int(r.cfg.LockTimeout.Milliseconds())
	_, err := script.Run(ctx, r.client, []string{key}, value, ttlMs).Result()
	return err
}

func (r *Runtime) getTenantID(ctx context.Context, namespace, id string) (string, error) {
	key := r.tenantKey(namespace, id)
	tenant, err := r.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	}
	return tenant, err
}

func (r *Runtime) setTenantID(ctx context.Context, namespace, id, tenantID string) error {
	key := r.tenantKey(namespace, id)
	return r.client.Set(ctx, key, tenantID, 0).Err()
}

func (r *Runtime) loadState(ctx context.Context, namespace, id string) (map[string][]byte, error) {
	key := r.stateKey(namespace, id)
	data, err := r.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return make(map[string][]byte), nil
	}
	if err != nil {
		return nil, err
	}

	var state map[string][]byte
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("unmarshal state: %w", err)
	}

	return state, nil
}

func (r *Runtime) saveState(ctx context.Context, namespace, id string, state map[string][]byte) error {
	key := r.stateKey(namespace, id)
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}

	return r.client.Set(ctx, key, data, 0).Err()
}

// Close releases resources.
func (r *Runtime) Close() error {
	return r.client.Close()
}

func generateLockID() string {
	return fmt.Sprintf("lock_%d_%d", time.Now().UnixNano(), time.Now().Nanosecond()%1000)
}
