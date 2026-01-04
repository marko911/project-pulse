package wasm

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// LoaderConfig contains configuration for the module loader.
type LoaderConfig struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	UseSSL    bool
}

// ModuleLoader loads WASM modules from S3/MinIO.
type ModuleLoader struct {
	cfg     LoaderConfig
	client  *minio.Client
	runtime *Runtime
	logger  *slog.Logger

	// Local cache of loaded modules
	cacheMu sync.RWMutex
	cache   map[string][]byte
}

// NewModuleLoader creates a new module loader.
func NewModuleLoader(cfg LoaderConfig, logger *slog.Logger) (*ModuleLoader, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	return &ModuleLoader{
		cfg:    cfg,
		client: client,
		logger: logger,
		cache:  make(map[string][]byte),
	}, nil
}

// SetRuntime sets the runtime for module compilation.
func (l *ModuleLoader) SetRuntime(runtime *Runtime) {
	l.runtime = runtime
}

// Load loads and compiles a WASM module by function ID.
func (l *ModuleLoader) Load(ctx context.Context, functionID string) (*CompiledModule, error) {
	// Construct the object key (e.g., "functions/{functionID}/module.wasm")
	objectKey := fmt.Sprintf("functions/%s/module.wasm", functionID)

	// Check if already compiled in runtime cache
	if l.runtime != nil {
		l.runtime.cacheMu.RLock()
		if cached, ok := l.runtime.cache[functionID]; ok {
			l.runtime.cacheMu.RUnlock()
			return cached, nil
		}
		l.runtime.cacheMu.RUnlock()
	}

	// Check local byte cache
	l.cacheMu.RLock()
	wasmBytes, cached := l.cache[functionID]
	l.cacheMu.RUnlock()

	if !cached {
		// Download from S3/MinIO
		l.logger.Debug("downloading module from storage",
			"function_id", functionID,
			"bucket", l.cfg.Bucket,
			"key", objectKey,
		)

		obj, err := l.client.GetObject(ctx, l.cfg.Bucket, objectKey, minio.GetObjectOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to get object: %w", err)
		}
		defer obj.Close()

		var buf bytes.Buffer
		if _, err := io.Copy(&buf, obj); err != nil {
			return nil, fmt.Errorf("failed to read object: %w", err)
		}

		wasmBytes = buf.Bytes()

		// Cache the bytes
		l.cacheMu.Lock()
		l.cache[functionID] = wasmBytes
		l.cacheMu.Unlock()
	}

	// Compile the module
	if l.runtime == nil {
		return nil, fmt.Errorf("runtime not set on loader")
	}

	return l.runtime.Compile(functionID, wasmBytes)
}

// Preload downloads and caches a module without compiling.
func (l *ModuleLoader) Preload(ctx context.Context, functionID string) error {
	objectKey := fmt.Sprintf("functions/%s/module.wasm", functionID)

	l.logger.Debug("preloading module",
		"function_id", functionID,
		"bucket", l.cfg.Bucket,
		"key", objectKey,
	)

	obj, err := l.client.GetObject(ctx, l.cfg.Bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to get object: %w", err)
	}
	defer obj.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, obj); err != nil {
		return fmt.Errorf("failed to read object: %w", err)
	}

	l.cacheMu.Lock()
	l.cache[functionID] = buf.Bytes()
	l.cacheMu.Unlock()

	return nil
}

// Invalidate removes a module from the cache.
func (l *ModuleLoader) Invalidate(functionID string) {
	l.cacheMu.Lock()
	delete(l.cache, functionID)
	l.cacheMu.Unlock()

	if l.runtime != nil {
		l.runtime.cacheMu.Lock()
		delete(l.runtime.cache, functionID)
		l.runtime.cacheMu.Unlock()
	}

	l.logger.Debug("invalidated module cache", "function_id", functionID)
}

// ListFunctions lists available functions in the bucket.
func (l *ModuleLoader) ListFunctions(ctx context.Context) ([]string, error) {
	var functions []string

	objectCh := l.client.ListObjects(ctx, l.cfg.Bucket, minio.ListObjectsOptions{
		Prefix:    "functions/",
		Recursive: false,
	})

	for obj := range objectCh {
		if obj.Err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", obj.Err)
		}
		// Extract function ID from path like "functions/{id}/"
		if len(obj.Key) > 10 {
			funcID := obj.Key[10 : len(obj.Key)-1] // Remove "functions/" prefix and trailing "/"
			functions = append(functions, funcID)
		}
	}

	return functions, nil
}

// UploadModule uploads a WASM module to storage.
func (l *ModuleLoader) UploadModule(ctx context.Context, functionID string, wasmBytes []byte) error {
	objectKey := fmt.Sprintf("functions/%s/module.wasm", functionID)

	_, err := l.client.PutObject(ctx, l.cfg.Bucket, objectKey, bytes.NewReader(wasmBytes), int64(len(wasmBytes)), minio.PutObjectOptions{
		ContentType: "application/wasm",
	})
	if err != nil {
		return fmt.Errorf("failed to upload module: %w", err)
	}

	l.logger.Info("uploaded module",
		"function_id", functionID,
		"size", len(wasmBytes),
	)

	// Invalidate any cached version
	l.Invalidate(functionID)

	return nil
}
