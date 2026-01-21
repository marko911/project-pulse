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

type Server struct {
	cfg    Config
	logger *slog.Logger
	minio  *minio.Client
	redis  *redis.Client
	db     *storage.DB
}

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

const (
	keyFunctionMeta   = "fn:meta:"
	keyTenantFuncs    = "fn:tenant:"
	keyFunctionByName = "fn:name:"
)

func NewServer(ctx context.Context, cfg Config, logger *slog.Logger) (*Server, error) {
	minioClient, err := minio.New(cfg.MinIOEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinIOAccessKey, cfg.MinIOSecretKey, ""),
		Secure: cfg.MinIOUseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("create minio client: %w", err)
	}

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

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
	})

	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

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

func (s *Server) Router() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", s.handleHealth)

	mux.HandleFunc("POST /api/v1/functions", s.handleUploadFunction)
	mux.HandleFunc("GET /api/v1/functions", s.handleListFunctions)
	mux.HandleFunc("GET /api/v1/functions/{id}", s.handleGetFunction)
	mux.HandleFunc("DELETE /api/v1/functions/{id}", s.handleDeleteFunction)

	return mux
}

func (s *Server) Close() error {
	s.db.Close()
	return s.redis.Close()
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}

func (s *Server) handleUploadFunction(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		tenantID = "default"
	}

	if err := r.ParseMultipartForm(50 << 20); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid multipart form: "+err.Error())
		return
	}

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

	file, header, err := r.FormFile("file")
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "file is required: "+err.Error())
		return
	}
	defer file.Close()

	if !strings.HasSuffix(header.Filename, ".wasm") {
		s.errorResponse(w, http.StatusBadRequest, "file must be a .wasm module")
		return
	}

	funcID := generateFunctionID()
	objectKey := fmt.Sprintf("%s/%s/%s.wasm", tenantID, funcID, version)

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

	if err := s.storeMetadata(ctx, meta); err != nil {
		s.minio.RemoveObject(ctx, s.cfg.MinIOBucket, objectKey, minio.RemoveObjectOptions{})
		s.logger.Error("failed to store metadata", "error", err)
		s.errorResponse(w, http.StatusInternalServerError, "failed to store metadata")
		return
	}

	s.jsonResponse(w, http.StatusCreated, meta)
}

func (s *Server) handleListFunctions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		tenantID = "default"
	}

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

	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		tenantID = "default"
	}
	if meta.TenantID != tenantID {
		s.errorResponse(w, http.StatusNotFound, "function not found")
		return
	}

	if err := s.minio.RemoveObject(ctx, s.cfg.MinIOBucket, meta.ObjectKey, minio.RemoveObjectOptions{}); err != nil {
		s.logger.Error("failed to delete object", "error", err)
		s.errorResponse(w, http.StatusInternalServerError, "failed to delete function")
		return
	}

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

	pipe.Set(ctx, keyFunctionMeta+meta.ID, data, 0)

	pipe.SAdd(ctx, keyTenantFuncs+meta.TenantID, meta.ID)

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

	pipe.Del(ctx, keyFunctionMeta+meta.ID)

	pipe.SRem(ctx, keyTenantFuncs+meta.TenantID, meta.ID)

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

func (s *Server) DownloadURL(ctx context.Context, meta *FunctionMetadata, expiry time.Duration) (string, error) {
	url, err := s.minio.PresignedGetObject(ctx, s.cfg.MinIOBucket, meta.ObjectKey, expiry, nil)
	if err != nil {
		return "", fmt.Errorf("generate presigned url: %w", err)
	}
	return url.String(), nil
}

func (s *Server) GetModuleReader(ctx context.Context, meta *FunctionMetadata) (io.ReadCloser, error) {
	obj, err := s.minio.GetObject(ctx, s.cfg.MinIOBucket, meta.ObjectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get object: %w", err)
	}
	return obj, nil
}
