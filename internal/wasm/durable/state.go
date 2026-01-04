package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Errors
var (
	ErrObjectLocked   = errors.New("durable object is locked by another instance")
	ErrTenantMismatch = errors.New("tenant ID does not match object owner")
	ErrObjectClosed   = errors.New("durable object has been closed")
)

// Object represents a single Durable Object instance.
// It provides exclusive access to state and automatic persistence.
type Object struct {
	runtime   *Runtime
	namespace string
	id        string
	tenantID  string
	lockKey   string
	lockValue string

	mu       sync.RWMutex
	state    map[string][]byte
	dirty    bool
	closed   bool

	// Lock renewal
	renewDone chan struct{}
}

// ID returns the object's unique identifier.
func (o *Object) ID() string {
	return o.id
}

// Namespace returns the object's namespace.
func (o *Object) Namespace() string {
	return o.namespace
}

// TenantID returns the tenant that owns this object.
func (o *Object) TenantID() string {
	return o.tenantID
}

// Get retrieves a value from state.
func (o *Object) Get(key string) ([]byte, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	val, ok := o.state[key]
	return val, ok
}

// GetString retrieves a string value from state.
func (o *Object) GetString(key string) (string, bool) {
	val, ok := o.Get(key)
	if !ok {
		return "", false
	}
	return string(val), true
}

// GetJSON retrieves and unmarshals a JSON value from state.
func (o *Object) GetJSON(key string, v interface{}) error {
	val, ok := o.Get(key)
	if !ok {
		return fmt.Errorf("key not found: %s", key)
	}
	return json.Unmarshal(val, v)
}

// Put stores a value in state.
func (o *Object) Put(key string, value []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return ErrObjectClosed
	}

	o.state[key] = value
	o.dirty = true
	return nil
}

// PutString stores a string value in state.
func (o *Object) PutString(key, value string) error {
	return o.Put(key, []byte(value))
}

// PutJSON marshals and stores a JSON value in state.
func (o *Object) PutJSON(key string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return o.Put(key, data)
}

// Delete removes a key from state.
func (o *Object) Delete(key string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return ErrObjectClosed
	}

	delete(o.state, key)
	o.dirty = true
	return nil
}

// List returns all keys in state.
func (o *Object) List() []string {
	o.mu.RLock()
	defer o.mu.RUnlock()

	keys := make([]string, 0, len(o.state))
	for k := range o.state {
		keys = append(keys, k)
	}
	return keys
}

// ListPrefix returns all keys with the given prefix.
func (o *Object) ListPrefix(prefix string) []string {
	o.mu.RLock()
	defer o.mu.RUnlock()

	var keys []string
	for k := range o.state {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}
	return keys
}

// Transaction executes a function with exclusive access to state.
// Changes are automatically saved on success.
// NOTE: The function should use the provided StateWriter to modify state.
func (o *Object) Transaction(ctx context.Context, fn func(s StateWriter) error) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return ErrObjectClosed
	}

	// Create a state writer that doesn't require locking
	sw := &stateWriter{obj: o}

	// Execute the transaction function
	if err := fn(sw); err != nil {
		return err
	}

	// Save if dirty
	if o.dirty {
		if err := o.runtime.saveState(ctx, o.namespace, o.id, o.state); err != nil {
			return fmt.Errorf("save state: %w", err)
		}
		o.dirty = false
	}

	return nil
}

// StateWriter provides write access to state within a transaction.
type StateWriter interface {
	Put(key string, value []byte)
	PutString(key, value string)
	Delete(key string)
}

// stateWriter implements StateWriter for use within transactions.
type stateWriter struct {
	obj *Object
}

func (s *stateWriter) Put(key string, value []byte) {
	s.obj.state[key] = value
	s.obj.dirty = true
}

func (s *stateWriter) PutString(key, value string) {
	s.Put(key, []byte(value))
}

func (s *stateWriter) Delete(key string) {
	delete(s.obj.state, key)
	s.obj.dirty = true
}

// Sync persists any pending state changes.
func (o *Object) Sync(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if !o.dirty {
		return nil
	}

	if err := o.runtime.saveState(ctx, o.namespace, o.id, o.state); err != nil {
		return fmt.Errorf("save state: %w", err)
	}

	o.dirty = false
	return nil
}

// StartRenewal begins automatic lock renewal in the background.
// This should be called for long-running operations.
func (o *Object) StartRenewal(ctx context.Context) {
	o.renewDone = make(chan struct{})

	go func() {
		// Renew at 1/3 of the lock timeout
		interval := o.runtime.cfg.LockTimeout / 3
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-o.renewDone:
				return
			case <-ticker.C:
				if err := o.runtime.extendLock(ctx, o.lockKey, o.lockValue); err != nil {
					// Lock lost, nothing we can do
					return
				}
			}
		}
	}()
}

// StopRenewal stops automatic lock renewal.
func (o *Object) StopRenewal() {
	if o.renewDone != nil {
		close(o.renewDone)
		o.renewDone = nil
	}
}

// Close releases the object and persists state.
func (o *Object) Close(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil
	}

	o.closed = true

	// Stop renewal if running
	if o.renewDone != nil {
		close(o.renewDone)
		o.renewDone = nil
	}

	// Save state
	if o.dirty {
		if err := o.runtime.saveState(ctx, o.namespace, o.id, o.state); err != nil {
			// Still release the lock even if save fails
			o.runtime.releaseLock(ctx, o.lockKey, o.lockValue)
			return fmt.Errorf("save state: %w", err)
		}
	}

	// Release the lock
	if err := o.runtime.releaseLock(ctx, o.lockKey, o.lockValue); err != nil {
		return fmt.Errorf("release lock: %w", err)
	}

	// Remove from cache
	key := o.runtime.objectKey(o.namespace, o.id)
	o.runtime.objects.Delete(key)

	return nil
}

// Storage provides a simplified state storage interface for WASM.
type Storage struct {
	obj *Object
}

// NewStorage creates a Storage wrapper for WASM access.
func NewStorage(obj *Object) *Storage {
	return &Storage{obj: obj}
}

// Get retrieves a value by key.
func (s *Storage) Get(key string) ([]byte, error) {
	val, ok := s.obj.Get(key)
	if !ok {
		return nil, nil // Return nil for not found (WASM convention)
	}
	return val, nil
}

// Put stores a value by key.
func (s *Storage) Put(key string, value []byte) error {
	return s.obj.Put(key, value)
}

// Delete removes a key.
func (s *Storage) Delete(key string) error {
	return s.obj.Delete(key)
}

// List returns all keys.
func (s *Storage) List(prefix string) ([]string, error) {
	if prefix == "" {
		return s.obj.List(), nil
	}
	return s.obj.ListPrefix(prefix), nil
}
