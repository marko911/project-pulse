package durable

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestObject_StateOperations(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}
	defer mr.Close()

	cfg := DefaultConfig()
	cfg.RedisAddr = mr.Addr()
	cfg.LockTimeout = 5 * time.Second

	runtime, err := NewRuntime(cfg)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}
	defer runtime.Close()

	ctx := context.Background()

	obj, err := runtime.Get(ctx, "test-ns", "obj-1", "tenant-1")
	if err != nil {
		t.Fatalf("Failed to get object: %v", err)
	}
	defer obj.Close(ctx)

	if err := obj.Put("key1", []byte("value1")); err != nil {
		t.Errorf("Put failed: %v", err)
	}

	val, ok := obj.Get("key1")
	if !ok || string(val) != "value1" {
		t.Errorf("Get failed: got %v, %v", string(val), ok)
	}

	if err := obj.PutString("key2", "hello"); err != nil {
		t.Errorf("PutString failed: %v", err)
	}

	str, ok := obj.GetString("key2")
	if !ok || str != "hello" {
		t.Errorf("GetString failed: got %v, %v", str, ok)
	}

	type TestData struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	testData := TestData{Name: "test", Value: 42}
	if err := obj.PutJSON("key3", testData); err != nil {
		t.Errorf("PutJSON failed: %v", err)
	}

	var retrieved TestData
	if err := obj.GetJSON("key3", &retrieved); err != nil {
		t.Errorf("GetJSON failed: %v", err)
	}
	if retrieved.Name != "test" || retrieved.Value != 42 {
		t.Errorf("GetJSON returned wrong value: %+v", retrieved)
	}

	if err := obj.Delete("key1"); err != nil {
		t.Errorf("Delete failed: %v", err)
	}

	_, ok = obj.Get("key1")
	if ok {
		t.Error("Key should not exist after delete")
	}

	keys := obj.List()
	if len(keys) != 2 {
		t.Errorf("List returned wrong number of keys: %d", len(keys))
	}

	if err := obj.PutString("prefix:a", "a"); err != nil {
		t.Errorf("PutString failed: %v", err)
	}
	if err := obj.PutString("prefix:b", "b"); err != nil {
		t.Errorf("PutString failed: %v", err)
	}

	prefixKeys := obj.ListPrefix("prefix:")
	if len(prefixKeys) != 2 {
		t.Errorf("ListPrefix returned wrong number of keys: %d", len(prefixKeys))
	}
}

func TestObject_Persistence(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}
	defer mr.Close()

	cfg := DefaultConfig()
	cfg.RedisAddr = mr.Addr()
	cfg.LockTimeout = 5 * time.Second

	runtime, err := NewRuntime(cfg)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	ctx := context.Background()

	obj1, err := runtime.Get(ctx, "persist-ns", "persist-1", "tenant-1")
	if err != nil {
		t.Fatalf("Failed to get object: %v", err)
	}

	if err := obj1.PutString("persistent", "data"); err != nil {
		t.Errorf("PutString failed: %v", err)
	}

	if err := obj1.Close(ctx); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	obj2, err := runtime.Get(ctx, "persist-ns", "persist-1", "tenant-1")
	if err != nil {
		t.Fatalf("Failed to get object again: %v", err)
	}
	defer obj2.Close(ctx)

	val, ok := obj2.GetString("persistent")
	if !ok || val != "data" {
		t.Errorf("Persisted data not found: got %v, %v", val, ok)
	}

	runtime.Close()
}

func TestObject_TenantIsolation(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}
	defer mr.Close()

	cfg := DefaultConfig()
	cfg.RedisAddr = mr.Addr()
	cfg.LockTimeout = 5 * time.Second

	runtime, err := NewRuntime(cfg)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}
	defer runtime.Close()

	ctx := context.Background()

	obj1, err := runtime.Get(ctx, "tenant-ns", "shared-obj", "tenant-1")
	if err != nil {
		t.Fatalf("Failed to get object for tenant-1: %v", err)
	}

	if err := obj1.PutString("data", "tenant1-data"); err != nil {
		t.Errorf("PutString failed: %v", err)
	}

	if err := obj1.Close(ctx); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	_, err = runtime.Get(ctx, "tenant-ns", "shared-obj", "tenant-2")
	if err != ErrTenantMismatch {
		t.Errorf("Expected ErrTenantMismatch, got: %v", err)
	}

	obj3, err := runtime.Get(ctx, "tenant-ns", "shared-obj", "tenant-1")
	if err != nil {
		t.Fatalf("Failed to get object for tenant-1 again: %v", err)
	}
	defer obj3.Close(ctx)

	val, ok := obj3.GetString("data")
	if !ok || val != "tenant1-data" {
		t.Errorf("Wrong data: got %v, %v", val, ok)
	}
}

func TestObject_Transaction(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}
	defer mr.Close()

	cfg := DefaultConfig()
	cfg.RedisAddr = mr.Addr()
	cfg.LockTimeout = 5 * time.Second

	runtime, err := NewRuntime(cfg)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}
	defer runtime.Close()

	ctx := context.Background()

	obj, err := runtime.Get(ctx, "tx-ns", "tx-obj", "tenant-1")
	if err != nil {
		t.Fatalf("Failed to get object: %v", err)
	}
	defer obj.Close(ctx)

	err = obj.Transaction(ctx, func(s StateWriter) error {
		s.PutString("counter", "0")
		s.PutString("name", "test")
		return nil
	})
	if err != nil {
		t.Errorf("Transaction failed: %v", err)
	}

	val, _ := obj.GetString("counter")
	if val != "0" {
		t.Errorf("Expected counter=0, got %v", val)
	}
}

func TestStorage_WASMInterface(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}
	defer mr.Close()

	cfg := DefaultConfig()
	cfg.RedisAddr = mr.Addr()
	cfg.LockTimeout = 5 * time.Second

	runtime, err := NewRuntime(cfg)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}
	defer runtime.Close()

	ctx := context.Background()

	obj, err := runtime.Get(ctx, "wasm-ns", "wasm-obj", "tenant-1")
	if err != nil {
		t.Fatalf("Failed to get object: %v", err)
	}
	defer obj.Close(ctx)

	storage := NewStorage(obj)

	if err := storage.Put("key", []byte("value")); err != nil {
		t.Errorf("Storage.Put failed: %v", err)
	}

	val, err := storage.Get("key")
	if err != nil || string(val) != "value" {
		t.Errorf("Storage.Get failed: %v, %v", err, string(val))
	}

	val, err = storage.Get("nonexistent")
	if err != nil || val != nil {
		t.Errorf("Storage.Get for nonexistent key should return nil: %v, %v", err, val)
	}

	storage.Put("list:a", []byte("a"))
	storage.Put("list:b", []byte("b"))

	keys, err := storage.List("list:")
	if err != nil || len(keys) != 2 {
		t.Errorf("Storage.List failed: %v, %d", err, len(keys))
	}

	if err := storage.Delete("key"); err != nil {
		t.Errorf("Storage.Delete failed: %v", err)
	}

	val, _ = storage.Get("key")
	if val != nil {
		t.Error("Key should be deleted")
	}
}
