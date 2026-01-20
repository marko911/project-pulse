package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/redis/go-redis/v9"

	"github.com/marko911/project-pulse/internal/platform/storage"
)

// Server handles function API requests.
type Server struct {
	cfg    Config
	logger *slog.Logger
	minio  *minio.Client
	redis  *redis.Client
	db     *storage.DB
}

// FunctionMetadata stores information about a deployed function.
type FunctionMetadata struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Version     string    `json:"version"`
	TenantID    string    `json:"tenant_id"`
	Description string    `json:"description,omitempty"`
	ObjectKey   string    `json:"object_key"`
	Size        int64     `json:"size"`
	ContentType string    `json:"content_type"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Redis key patterns
const (
	keyFunctionMeta   = "fn:meta:"   // fn:meta:{id} -> FunctionMetadata JSON
	keyTenantFuncs    = "fn:tenant:" // fn:tenant:{tenant_id} -> set of function IDs
	keyFunctionByName = "fn:name:"   // fn:name:{tenant_id}:{name} -> function ID
)

// NewServer creates a new function API server.
func NewServer(ctx context.Context, cfg Config, logger *slog.Logger) (*Server, error) {
	// Initialize MinIO client
	minioClient, err := minio.New(cfg.MinIOEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinIOAccessKey, cfg.MinIOSecretKey, ""),
		Secure: cfg.MinIOUseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("create minio client: %w", err)
	}

	// Ensure bucket exists
	exists, err := minioClient.BucketExists(ctx, cfg.MinIOBucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket: %w", err)
	}
	if !exists {
		if err := minioClient.MakeBucket(ctx, cfg.MinIOBucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("create bucket: %w", err)
		}
		logger.Info("created bucket", "bucket", cfg.MinIOBucket)
	}

	// Initialize Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
	})

	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	// Initialize Database
	dbCfg := storage.Config{
		Host:     cfg.DBHost,
		Port:     cfg.DBPort,
		User:     cfg.DBUser,
		Password: cfg.DBPassword,
		Database: cfg.DBName,
		SSLMode:  "disable",
	}

	db, err := storage.New(ctx, dbCfg)
	if err != nil {
		redisClient.Close()
		return nil, fmt.Errorf("database connect: %w", err)
	}

	// Run migrations
	if err := db.Migrate(ctx); err != nil {
		db.Close()
		redisClient.Close()
		return nil, fmt.Errorf("database migrate: %w", err)
	}

	logger.Info("connected to database and applied migrations")

	return &Server{
		cfg:    cfg,
		logger: logger,
		minio:  minioClient,
		redis:  redisClient,
		db:     db,
	}, nil
}

// Router returns the HTTP router for the server.
func (s *Server) Router() http.Handler {
	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("GET /health", s.handleHealth)

	// Function management
	mux.HandleFunc("POST /api/v1/functions", s.handleUploadFunction)
	mux.HandleFunc("GET /api/v1/functions", s.handleListFunctions)
	mux.HandleFunc("GET /api/v1/functions/{id}", s.handleGetFunction)
	mux.HandleFunc("DELETE /api/v1/functions/{id}", s.handleDeleteFunction)

	return mux
}

// Close releases resources.
func (s *Server) Close() error {
	s.db.Close()
	return s.redis.Close()
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}

// handleUploadFunction handles POST /api/v1/functions
// Accepts multipart form with:
// - file: WASM module binary
// - name: function name
// - version: optional version string
// - description: optional description
func (s *Server) handleUploadFunction(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get tenant ID from header (in production, this would come from auth)
	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		tenantID = "default"
	}

	// Parse multipart form (max 50MB)
	if err := r.ParseMultipartForm(50 << 20); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid multipart form: "+err.Error())
		return
	}

	// Get form values
	name := r.FormValue("name")
	if name == "" {
		s.errorResponse(w, http.StatusBadRequest, "name is required")
		return
	}

	version := r.FormValue("version")
	if version == "" {
		version = "1.0.0"
	}

	description := r.FormValue("description")

	// Get the file
	file, header, err := r.FormFile("file")
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "file is required: "+err.Error())
		return
	}
	defer file.Close()

	// Validate file extension
	if !strings.HasSuffix(header.Filename, ".wasm") {
		s.errorResponse(w, http.StatusBadRequest, "file must be a .wasm module")
		return
	}

	// Generate function ID
	funcID := generateFunctionID()
	objectKey := fmt.Sprintf("%s/%s/%s.wasm", tenantID, funcID, version)

	// Upload to MinIO
	info, err := s.minio.PutObject(ctx, s.cfg.MinIOBucket, objectKey, file, header.Size, minio.PutObjectOptions{
		ContentType: "application/wasm",
	})
	if err != nil {
		s.logger.Error("failed to upload to minio", "error", err)
		s.errorResponse(w, http.StatusInternalServerError, "failed to store module")
		return
	}

	s.logger.Info("uploaded wasm module",
		"function_id", funcID,
		"object_key", objectKey,
		"size", info.Size,
	)

	// Create metadata
	now := time.Now().UTC()
	meta := FunctionMetadata{
		ID:          funcID,
		Name:        name,
		Version:     version,
		TenantID:    tenantID,
		Description: description,
		ObjectKey:   objectKey,
		Size:        info.Size,
		ContentType: "application/wasm",
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	// Store metadata in Redis
	if err := s.storeMetadata(ctx, meta); err != nil {
		// Rollback: delete the uploaded object
		s.minio.RemoveObject(ctx, s.cfg.MinIOBucket, objectKey, minio.RemoveObjectOptions{})
		s.logger.Error("failed to store metadata", "error", err)
		s.errorResponse(w, http.StatusInternalServerError, "failed to store metadata")
		return
	}

	s.jsonResponse(w, http.StatusCreated, meta)
}

// handleListFunctions handles GET /api/v1/functions
func (s *Server) handleListFunctions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		tenantID = "default"
	}

	// Get function IDs for tenant
	funcIDs, err := s.redis.SMembers(ctx, keyTenantFuncs+tenantID).Result()
	if err != nil {
		s.logger.Error("failed to list functions", "error", err)
		s.errorResponse(w, http.StatusInternalServerError, "failed to list functions")
		return
	}

	functions := make([]FunctionMetadata, 0, len(funcIDs))
	for _, id := range funcIDs {
		meta, err := s.getMetadata(ctx, id)
		if err != nil {
			s.logger.Warn("failed to get function metadata", "id", id, "error", err)
			continue
		}
		functions = append(functions, *meta)
	}

	s.jsonResponse(w, http.StatusOK, map[string]interface{}{
		"functions": functions,
		"count":     len(functions),
	})
}

// handleGetFunction handles GET /api/v1/functions/{id}
func (s *Server) handleGetFunction(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	funcID := r.PathValue("id")

	meta, err := s.getMetadata(ctx, funcID)
	if err == redis.Nil {
		s.errorResponse(w, http.StatusNotFound, "function not found")
		return
	}
	if err != nil {
		s.logger.Error("failed to get function", "error", err)
		s.errorResponse(w, http.StatusInternalServerError, "failed to get function")
		return
	}

	// Verify tenant access
	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		tenantID = "default"
	}
	if meta.TenantID != tenantID {
		s.errorResponse(w, http.StatusNotFound, "function not found")
		return
	}

	s.jsonResponse(w, http.StatusOK, meta)
}

// handleDeleteFunction handles DELETE /api/v1/functions/{id}
func (s *Server) handleDeleteFunction(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	funcID := r.PathValue("id")

	meta, err := s.getMetadata(ctx, funcID)
	if err == redis.Nil {
		s.errorResponse(w, http.StatusNotFound, "function not found")
		return
	}
	if err != nil {
		s.logger.Error("failed to get function", "error", err)
		s.errorResponse(w, http.StatusInternalServerError, "failed to get function")
		return
	}

	// Verify tenant access
	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		tenantID = "default"
	}
	if meta.TenantID != tenantID {
		s.errorResponse(w, http.StatusNotFound, "function not found")
		return
	}

	// Delete from MinIO
	if err := s.minio.RemoveObject(ctx, s.cfg.MinIOBucket, meta.ObjectKey, minio.RemoveObjectOptions{}); err != nil {
		s.logger.Error("failed to delete object", "error", err)
		s.errorResponse(w, http.StatusInternalServerError, "failed to delete function")
		return
	}

	// Delete metadata
	if err := s.deleteMetadata(ctx, meta); err != nil {
		s.logger.Error("failed to delete metadata", "error", err)
		s.errorResponse(w, http.StatusInternalServerError, "failed to delete metadata")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) storeMetadata(ctx context.Context, meta FunctionMetadata) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	pipe := s.redis.Pipeline()

	// Store metadata
	pipe.Set(ctx, keyFunctionMeta+meta.ID, data, 0)

	// Add to tenant's function set
	pipe.SAdd(ctx, keyTenantFuncs+meta.TenantID, meta.ID)

	// Index by name
	pipe.Set(ctx, keyFunctionByName+meta.TenantID+":"+meta.Name, meta.ID, 0)

	_, err = pipe.Exec(ctx)
	return err
}

func (s *Server) getMetadata(ctx context.Context, id string) (*FunctionMetadata, error) {
	data, err := s.redis.Get(ctx, keyFunctionMeta+id).Bytes()
	if err != nil {
		return nil, err
	}

	var meta FunctionMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshal metadata: %w", err)
	}

	return &meta, nil
}

func (s *Server) deleteMetadata(ctx context.Context, meta *FunctionMetadata) error {
	pipe := s.redis.Pipeline()

	// Delete metadata
	pipe.Del(ctx, keyFunctionMeta+meta.ID)

	// Remove from tenant's function set
	pipe.SRem(ctx, keyTenantFuncs+meta.TenantID, meta.ID)

	// Remove name index
	pipe.Del(ctx, keyFunctionByName+meta.TenantID+":"+meta.Name)

	_, err := pipe.Exec(ctx)
	return err
}

func (s *Server) errorResponse(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func (s *Server) jsonResponse(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func generateFunctionID() string {
	return fmt.Sprintf("fn_%d", time.Now().UnixNano())
}

// DownloadURL returns a presigned URL for downloading the function's WASM module.
func (s *Server) DownloadURL(ctx context.Context, meta *FunctionMetadata, expiry time.Duration) (string, error) {
	url, err := s.minio.PresignedGetObject(ctx, s.cfg.MinIOBucket, meta.ObjectKey, expiry, nil)
	if err != nil {
		return "", fmt.Errorf("generate presigned url: %w", err)
	}
	return url.String(), nil
}

// GetModuleReader returns a reader for the WASM module binary.
func (s *Server) GetModuleReader(ctx context.Context, meta *FunctionMetadata) (io.ReadCloser, error) {
	obj, err := s.minio.GetObject(ctx, s.cfg.MinIOBucket, meta.ObjectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get object: %w", err)
	}
	return obj, nil
}
