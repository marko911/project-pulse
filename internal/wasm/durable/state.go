package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrObjectLocked   = errors.New("durable object is locked by another instance")
	ErrTenantMismatch = errors.New("tenant ID does not match object owner")
	ErrObjectClosed   = errors.New("durable object has been closed")
)

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

	renewDone chan struct{}
}

func (o *Object) ID() string {
	return o.id
}

func (o *Object) Namespace() string {
	return o.namespace
}

func (o *Object) TenantID() string {
	return o.tenantID
}

func (o *Object) Get(key string) ([]byte, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	val, ok := o.state[key]
	return val, ok
}

func (o *Object) GetString(key string) (string, bool) {
	val, ok := o.Get(key)
	if !ok {
		return "", false
	}
	return string(val), true
}

func (o *Object) GetJSON(key string, v interface{}) error {
	val, ok := o.Get(key)
	if !ok {
		return fmt.Errorf("key not found: %s", key)
	}
	return json.Unmarshal(val, v)
}

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

func (o *Object) PutString(key, value string) error {
	return o.Put(key, []byte(value))
}

func (o *Object) PutJSON(key string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return o.Put(key, data)
}

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

func (o *Object) List() []string {
	o.mu.RLock()
	defer o.mu.RUnlock()

	keys := make([]string, 0, len(o.state))
	for k := range o.state {
		keys = append(keys, k)
	}
	return keys
}

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

func (o *Object) Transaction(ctx context.Context, fn func(s StateWriter) error) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return ErrObjectClosed
	}

	sw := &stateWriter{obj: o}

	if err := fn(sw); err != nil {
		return err
	}

	if o.dirty {
		if err := o.runtime.saveState(ctx, o.namespace, o.id, o.state); err != nil {
			return fmt.Errorf("save state: %w", err)
		}
		o.dirty = false
	}

	return nil
}

type StateWriter interface {
	Put(key string, value []byte)
	PutString(key, value string)
	Delete(key string)
}

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

func (o *Object) StartRenewal(ctx context.Context) {
	o.renewDone = make(chan struct{})

	go func() {
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
					return
				}
			}
		}
	}()
}

func (o *Object) StopRenewal() {
	if o.renewDone != nil {
		close(o.renewDone)
		o.renewDone = nil
	}
}

func (o *Object) Close(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil
	}

	o.closed = true

	if o.renewDone != nil {
		close(o.renewDone)
		o.renewDone = nil
	}

	if o.dirty {
		if err := o.runtime.saveState(ctx, o.namespace, o.id, o.state); err != nil {
			o.runtime.releaseLock(ctx, o.lockKey, o.lockValue)
			return fmt.Errorf("save state: %w", err)
		}
	}

	if err := o.runtime.releaseLock(ctx, o.lockKey, o.lockValue); err != nil {
		return fmt.Errorf("release lock: %w", err)
	}

	key := o.runtime.objectKey(o.namespace, o.id)
	o.runtime.objects.Delete(key)

	return nil
}

type Storage struct {
	obj *Object
}

func NewStorage(obj *Object) *Storage {
	return &Storage{obj: obj}
}

func (s *Storage) Get(key string) ([]byte, error) {
	val, ok := s.obj.Get(key)
	if !ok {
		return nil, nil
	}
	return val, nil
}

func (s *Storage) Put(key string, value []byte) error {
	return s.obj.Put(key, value)
}

func (s *Storage) Delete(key string) error {
	return s.obj.Delete(key)
}

func (s *Storage) List(prefix string) ([]string, error) {
	if prefix == "" {
		return s.obj.List(), nil
	}
	return s.obj.ListPrefix(prefix), nil
}
