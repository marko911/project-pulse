package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/marko911/project-pulse/internal/platform/storage"
	"github.com/marko911/project-pulse/internal/wasm"
)

// FunctionAPI handles function registry endpoints.
type FunctionAPI struct {
	repo   *storage.FunctionRepository
	loader *wasm.ModuleLoader
	server *Server
}

// NewFunctionAPI creates a new function API handler.
func NewFunctionAPI(repo *storage.FunctionRepository, loader *wasm.ModuleLoader, server *Server) *FunctionAPI {
	return &FunctionAPI{
		repo:   repo,
		loader: loader,
		server: server,
	}
}

// RegisterRoutes registers function API routes.
func (a *FunctionAPI) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/functions", a.handleFunctions)
	mux.HandleFunc("/api/v1/functions/", a.handleFunction)
}

// handleFunctions handles /api/v1/functions (list/create).
func (a *FunctionAPI) handleFunctions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		a.listFunctions(w, r)
	case http.MethodPost:
		a.createFunction(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleFunction handles /api/v1/functions/{id} and sub-routes.
func (a *FunctionAPI) handleFunction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/functions/")
	parts := strings.Split(path, "/")

	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "function ID required", http.StatusBadRequest)
		return
	}

	functionID := parts[0]

	// Check for sub-routes
	if len(parts) > 1 {
		switch parts[1] {
		case "deploy":
			a.handleDeploy(w, r, functionID)
		case "triggers":
			a.handleTriggers(w, r, functionID)
		case "deployments":
			a.handleDeployments(w, r, functionID)
		case "stats":
			a.handleStats(w, r, functionID)
		case "logs":
			a.handleLogs(w, r, functionID)
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
		return
	}

	// Handle function CRUD
	switch r.Method {
	case http.MethodGet:
		a.getFunction(w, r, functionID)
	case http.MethodPut:
		a.updateFunction(w, r, functionID)
	case http.MethodDelete:
		a.deleteFunction(w, r, functionID)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// listFunctions returns a paginated list of functions for the tenant.
func (a *FunctionAPI) listFunctions(w http.ResponseWriter, r *http.Request) {
	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		a.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "X-Tenant-ID header required",
		})
		return
	}

	limit := 50
	offset := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		if l, err := strconv.Atoi(v); err == nil && l > 0 && l <= 100 {
			limit = l
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if o, err := strconv.Atoi(v); err == nil && o >= 0 {
			offset = o
		}
	}

	functions, err := a.repo.ListFunctions(r.Context(), tenantID, limit, offset)
	if err != nil {
		a.server.logger.Error("list functions error", "error", err)
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "internal error",
		})
		return
	}

	result := make([]map[string]interface{}, 0, len(functions))
	for _, f := range functions {
		result = append(result, functionToJSON(&f))
	}

	a.writeJSON(w, http.StatusOK, map[string]interface{}{
		"functions": result,
		"count":     len(result),
		"limit":     limit,
		"offset":    offset,
	})
}

// createFunction creates a new function.
func (a *FunctionAPI) createFunction(w http.ResponseWriter, r *http.Request) {
	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		a.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "X-Tenant-ID header required",
		})
		return
	}

	var body struct {
		Name           string `json:"name"`
		Description    string `json:"description"`
		RuntimeVersion string `json:"runtime_version"`
		MaxMemoryMB    int    `json:"max_memory_mb"`
		MaxCPUMs       int    `json:"max_cpu_ms"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		a.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "invalid request body",
		})
		return
	}

	if body.Name == "" {
		a.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "name is required",
		})
		return
	}

	// Check if function already exists
	existing, err := a.repo.GetFunctionByName(r.Context(), tenantID, body.Name)
	if err != nil {
		a.server.logger.Error("check existing function error", "error", err)
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "internal error",
		})
		return
	}
	if existing != nil {
		a.writeJSON(w, http.StatusConflict, map[string]interface{}{
			"error": "function already exists",
			"name":  body.Name,
		})
		return
	}

	// Set defaults
	runtimeVersion := body.RuntimeVersion
	if runtimeVersion == "" {
		runtimeVersion = "1.0"
	}
	maxMemoryMB := body.MaxMemoryMB
	if maxMemoryMB == 0 {
		maxMemoryMB = 128
	}
	maxCPUMs := body.MaxCPUMs
	if maxCPUMs == 0 {
		maxCPUMs = 5000
	}

	var description *string
	if body.Description != "" {
		description = &body.Description
	}

	f := &storage.Function{
		TenantID:       tenantID,
		Name:           body.Name,
		Description:    description,
		RuntimeVersion: runtimeVersion,
		MaxMemoryMB:    maxMemoryMB,
		MaxCPUMs:       maxCPUMs,
		Status:         storage.FunctionStatusActive,
	}

	if err := a.repo.CreateFunction(r.Context(), f); err != nil {
		a.server.logger.Error("create function error", "error", err)
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "failed to create function",
		})
		return
	}

	a.server.logger.Info("function created",
		"function_id", f.ID,
		"tenant_id", tenantID,
		"name", body.Name,
	)

	a.writeJSON(w, http.StatusCreated, functionToJSON(f))
}

// getFunction retrieves a function by ID.
func (a *FunctionAPI) getFunction(w http.ResponseWriter, r *http.Request, functionID string) {
	f, err := a.repo.GetFunction(r.Context(), functionID)
	if err != nil {
		a.server.logger.Error("get function error", "error", err)
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "internal error",
		})
		return
	}

	if f == nil {
		a.writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"error": "function not found",
			"id":    functionID,
		})
		return
	}

	a.writeJSON(w, http.StatusOK, functionToJSON(f))
}

// updateFunction updates a function.
func (a *FunctionAPI) updateFunction(w http.ResponseWriter, r *http.Request, functionID string) {
	f, err := a.repo.GetFunction(r.Context(), functionID)
	if err != nil {
		a.server.logger.Error("get function error", "error", err)
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "internal error",
		})
		return
	}

	if f == nil {
		a.writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"error": "function not found",
			"id":    functionID,
		})
		return
	}

	var body struct {
		Name           *string `json:"name"`
		Description    *string `json:"description"`
		RuntimeVersion *string `json:"runtime_version"`
		MaxMemoryMB    *int    `json:"max_memory_mb"`
		MaxCPUMs       *int    `json:"max_cpu_ms"`
		Status         *string `json:"status"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		a.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "invalid request body",
		})
		return
	}

	if body.Name != nil {
		f.Name = *body.Name
	}
	if body.Description != nil {
		f.Description = body.Description
	}
	if body.RuntimeVersion != nil {
		f.RuntimeVersion = *body.RuntimeVersion
	}
	if body.MaxMemoryMB != nil {
		f.MaxMemoryMB = *body.MaxMemoryMB
	}
	if body.MaxCPUMs != nil {
		f.MaxCPUMs = *body.MaxCPUMs
	}
	if body.Status != nil {
		switch *body.Status {
		case "active":
			f.Status = storage.FunctionStatusActive
		case "disabled":
			f.Status = storage.FunctionStatusDisabled
		default:
			a.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"error": "invalid status",
			})
			return
		}
	}

	if err := a.repo.UpdateFunction(r.Context(), f); err != nil {
		a.server.logger.Error("update function error", "error", err)
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "failed to update function",
		})
		return
	}

	a.server.logger.Info("function updated", "function_id", functionID)

	a.writeJSON(w, http.StatusOK, functionToJSON(f))
}

// deleteFunction soft-deletes a function.
func (a *FunctionAPI) deleteFunction(w http.ResponseWriter, r *http.Request, functionID string) {
	if err := a.repo.DeleteFunction(r.Context(), functionID); err != nil {
		if strings.Contains(err.Error(), "not found") {
			a.writeJSON(w, http.StatusNotFound, map[string]interface{}{
				"error": "function not found",
				"id":    functionID,
			})
			return
		}
		a.server.logger.Error("delete function error", "error", err)
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "failed to delete function",
		})
		return
	}

	a.server.logger.Info("function deleted", "function_id", functionID)

	a.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "deleted",
		"id":     functionID,
	})
}

// handleDeploy handles WASM module deployment.
func (a *FunctionAPI) handleDeploy(w http.ResponseWriter, r *http.Request, functionID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		a.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "X-Tenant-ID header required",
		})
		return
	}

	// Get function
	f, err := a.repo.GetFunction(r.Context(), functionID)
	if err != nil {
		a.server.logger.Error("get function error", "error", err)
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "internal error",
		})
		return
	}
	if f == nil {
		a.writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"error": "function not found",
			"id":    functionID,
		})
		return
	}

	// Verify tenant ownership
	if f.TenantID != tenantID {
		a.writeJSON(w, http.StatusForbidden, map[string]interface{}{
			"error": "not authorized to deploy to this function",
		})
		return
	}

	// Parse multipart form (max 50MB)
	if err := r.ParseMultipartForm(50 << 20); err != nil {
		a.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "failed to parse form: " + err.Error(),
		})
		return
	}

	file, header, err := r.FormFile("module")
	if err != nil {
		a.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "module file required",
		})
		return
	}
	defer file.Close()

	// Read the WASM module
	wasmBytes, err := io.ReadAll(file)
	if err != nil {
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "failed to read module",
		})
		return
	}

	// Validate WASM magic number
	if len(wasmBytes) < 4 || string(wasmBytes[:4]) != "\x00asm" {
		a.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "invalid WASM module: missing magic header",
		})
		return
	}

	// Calculate hash
	hash := sha256.Sum256(wasmBytes)
	hashStr := hex.EncodeToString(hash[:])

	// Upload to S3/MinIO
	if a.loader != nil {
		if err := a.loader.UploadModule(r.Context(), functionID, wasmBytes); err != nil {
			a.server.logger.Error("upload module error", "error", err)
			a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"error": "failed to upload module",
			})
			return
		}
	}

	// Get optional deployment note
	deploymentNote := r.FormValue("note")
	deployedBy := r.Header.Get("X-User-ID")

	var notePtr, deployedByPtr *string
	if deploymentNote != "" {
		notePtr = &deploymentNote
	}
	if deployedBy != "" {
		deployedByPtr = &deployedBy
	}

	// Create deployment record
	modulePath := "functions/" + functionID + "/module.wasm"
	deployment := &storage.Deployment{
		FunctionID:     functionID,
		TenantID:       tenantID,
		ModuleHash:     hashStr,
		ModuleSize:     len(wasmBytes),
		ModulePath:     modulePath,
		DeployedBy:     deployedByPtr,
		DeploymentNote: notePtr,
		Status:         storage.DeploymentStatusActive,
	}

	if err := a.repo.CreateDeployment(r.Context(), deployment); err != nil {
		a.server.logger.Error("create deployment error", "error", err)
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "failed to create deployment record",
		})
		return
	}

	a.server.logger.Info("function deployed",
		"function_id", functionID,
		"version", deployment.Version,
		"size", len(wasmBytes),
		"hash", hashStr[:12],
	)

	a.writeJSON(w, http.StatusCreated, map[string]interface{}{
		"status":       "deployed",
		"function_id":  functionID,
		"version":      deployment.Version,
		"module_hash":  hashStr,
		"module_size":  len(wasmBytes),
		"module_path":  modulePath,
		"filename":     header.Filename,
		"deployed_at":  time.Now().UTC().Format(time.RFC3339),
		"deployed_by":  deployedBy,
		"deployment_id": deployment.ID,
	})
}

// handleTriggers handles trigger management for a function.
func (a *FunctionAPI) handleTriggers(w http.ResponseWriter, r *http.Request, functionID string) {
	switch r.Method {
	case http.MethodGet:
		a.listTriggers(w, r, functionID)
	case http.MethodPost:
		a.createTrigger(w, r, functionID)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a *FunctionAPI) listTriggers(w http.ResponseWriter, r *http.Request, functionID string) {
	triggers, err := a.repo.ListTriggersByFunction(r.Context(), functionID)
	if err != nil {
		a.server.logger.Error("list triggers error", "error", err)
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "internal error",
		})
		return
	}

	result := make([]map[string]interface{}, 0, len(triggers))
	for _, t := range triggers {
		result = append(result, triggerToJSON(&t))
	}

	a.writeJSON(w, http.StatusOK, map[string]interface{}{
		"triggers": result,
		"count":    len(result),
	})
}

func (a *FunctionAPI) createTrigger(w http.ResponseWriter, r *http.Request, functionID string) {
	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		a.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "X-Tenant-ID header required",
		})
		return
	}

	// Verify function exists
	f, err := a.repo.GetFunction(r.Context(), functionID)
	if err != nil || f == nil {
		a.writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"error": "function not found",
		})
		return
	}

	var body struct {
		Name          string          `json:"name"`
		EventType     string          `json:"event_type"`
		FilterChain   *int16          `json:"filter_chain"`
		FilterAddress *string         `json:"filter_address"`
		FilterTopic   *string         `json:"filter_topic"`
		FilterJSON    json.RawMessage `json:"filter_json"`
		Priority      int             `json:"priority"`
		MaxRetries    int             `json:"max_retries"`
		TimeoutMs     int             `json:"timeout_ms"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		a.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "invalid request body",
		})
		return
	}

	if body.Name == "" || body.EventType == "" {
		a.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "name and event_type are required",
		})
		return
	}

	// Set defaults
	priority := body.Priority
	if priority == 0 {
		priority = 100
	}
	maxRetries := body.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}
	timeoutMs := body.TimeoutMs
	if timeoutMs == 0 {
		timeoutMs = 5000
	}

	t := &storage.Trigger{
		FunctionID:    functionID,
		TenantID:      tenantID,
		Name:          body.Name,
		EventType:     body.EventType,
		FilterChain:   body.FilterChain,
		FilterAddress: body.FilterAddress,
		FilterTopic:   body.FilterTopic,
		FilterJSON:    body.FilterJSON,
		Priority:      priority,
		MaxRetries:    maxRetries,
		TimeoutMs:     timeoutMs,
		Status:        storage.TriggerStatusActive,
	}

	if err := a.repo.CreateTrigger(r.Context(), t); err != nil {
		a.server.logger.Error("create trigger error", "error", err)
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "failed to create trigger",
		})
		return
	}

	a.server.logger.Info("trigger created",
		"trigger_id", t.ID,
		"function_id", functionID,
		"event_type", body.EventType,
	)

	a.writeJSON(w, http.StatusCreated, triggerToJSON(t))
}

// handleDeployments lists deployment history.
func (a *FunctionAPI) handleDeployments(w http.ResponseWriter, r *http.Request, functionID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	limit := 20
	if v := r.URL.Query().Get("limit"); v != "" {
		if l, err := strconv.Atoi(v); err == nil && l > 0 && l <= 100 {
			limit = l
		}
	}

	deployments, err := a.repo.ListDeployments(r.Context(), functionID, limit)
	if err != nil {
		a.server.logger.Error("list deployments error", "error", err)
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "internal error",
		})
		return
	}

	result := make([]map[string]interface{}, 0, len(deployments))
	for _, d := range deployments {
		result = append(result, deploymentToJSON(&d))
	}

	a.writeJSON(w, http.StatusOK, map[string]interface{}{
		"deployments": result,
		"count":       len(result),
	})
}

// handleStats returns invocation statistics.
func (a *FunctionAPI) handleStats(w http.ResponseWriter, r *http.Request, functionID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Default to last 24 hours
	since := time.Now().Add(-24 * time.Hour)
	if v := r.URL.Query().Get("since"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			since = t
		}
	}

	stats, err := a.repo.GetInvocationStats(r.Context(), functionID, since)
	if err != nil {
		a.server.logger.Error("get stats error", "error", err)
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "internal error",
		})
		return
	}

	a.writeJSON(w, http.StatusOK, map[string]interface{}{
		"function_id":            functionID,
		"since":                  since.UTC().Format(time.RFC3339),
		"total_invocations":      stats.TotalInvocations,
		"successful_invocations": stats.SuccessfulInvocations,
		"failed_invocations":     stats.FailedInvocations,
		"avg_duration_ms":        stats.AvgDurationMs,
		"max_duration_ms":        stats.MaxDurationMs,
		"avg_memory_bytes":       stats.AvgMemoryBytes,
	})
}

// handleLogs returns invocation logs for a function.
func (a *FunctionAPI) handleLogs(w http.ResponseWriter, r *http.Request, functionID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	limit := 50
	offset := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		if l, err := strconv.Atoi(v); err == nil && l > 0 && l <= 100 {
			limit = l
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if o, err := strconv.Atoi(v); err == nil && o >= 0 {
			offset = o
		}
	}

	invocations, err := a.repo.ListInvocations(r.Context(), functionID, limit, offset)
	if err != nil {
		a.server.logger.Error("list invocations error", "error", err)
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "internal error",
		})
		return
	}

	result := make([]map[string]interface{}, 0, len(invocations))
	for _, inv := range invocations {
		result = append(result, invocationToJSON(&inv))
	}

	a.writeJSON(w, http.StatusOK, map[string]interface{}{
		"function_id":  functionID,
		"invocations":  result,
		"count":        len(result),
		"limit":        limit,
		"offset":       offset,
	})
}

// writeJSON writes a JSON response.
func (a *FunctionAPI) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		a.server.logger.Error("JSON encode error", "error", err)
	}
}

// functionToJSON converts a Function to a JSON-friendly map.
func functionToJSON(f *storage.Function) map[string]interface{} {
	result := map[string]interface{}{
		"id":              f.ID,
		"tenant_id":       f.TenantID,
		"name":            f.Name,
		"runtime_version": f.RuntimeVersion,
		"max_memory_mb":   f.MaxMemoryMB,
		"max_cpu_ms":      f.MaxCPUMs,
		"current_version": f.CurrentVersion,
		"status":          string(f.Status),
		"created_at":      f.CreatedAt.UTC().Format(time.RFC3339),
		"updated_at":      f.UpdatedAt.UTC().Format(time.RFC3339),
	}
	if f.Description != nil {
		result["description"] = *f.Description
	}
	if f.ModuleHash != nil {
		result["module_hash"] = *f.ModuleHash
	}
	if f.ModuleSize != nil {
		result["module_size"] = *f.ModuleSize
	}
	return result
}

// triggerToJSON converts a Trigger to a JSON-friendly map.
func triggerToJSON(t *storage.Trigger) map[string]interface{} {
	result := map[string]interface{}{
		"id":          t.ID,
		"function_id": t.FunctionID,
		"tenant_id":   t.TenantID,
		"name":        t.Name,
		"event_type":  t.EventType,
		"priority":    t.Priority,
		"max_retries": t.MaxRetries,
		"timeout_ms":  t.TimeoutMs,
		"status":      string(t.Status),
		"created_at":  t.CreatedAt.UTC().Format(time.RFC3339),
		"updated_at":  t.UpdatedAt.UTC().Format(time.RFC3339),
	}
	if t.FilterChain != nil {
		result["filter_chain"] = *t.FilterChain
	}
	if t.FilterAddress != nil {
		result["filter_address"] = *t.FilterAddress
	}
	if t.FilterTopic != nil {
		result["filter_topic"] = *t.FilterTopic
	}
	if len(t.FilterJSON) > 0 {
		result["filter_json"] = json.RawMessage(t.FilterJSON)
	}
	return result
}

// deploymentToJSON converts a Deployment to a JSON-friendly map.
func deploymentToJSON(d *storage.Deployment) map[string]interface{} {
	result := map[string]interface{}{
		"id":          d.ID,
		"function_id": d.FunctionID,
		"tenant_id":   d.TenantID,
		"version":     d.Version,
		"module_hash": d.ModuleHash,
		"module_size": d.ModuleSize,
		"module_path": d.ModulePath,
		"status":      string(d.Status),
		"created_at":  d.CreatedAt.UTC().Format(time.RFC3339),
	}
	if d.SourceHash != nil {
		result["source_hash"] = *d.SourceHash
	}
	if d.DeployedBy != nil {
		result["deployed_by"] = *d.DeployedBy
	}
	if d.DeploymentNote != nil {
		result["deployment_note"] = *d.DeploymentNote
	}
	if d.ActivatedAt != nil {
		result["activated_at"] = d.ActivatedAt.UTC().Format(time.RFC3339)
	}
	return result
}

// invocationToJSON converts an Invocation to a JSON-friendly map.
func invocationToJSON(inv *storage.Invocation) map[string]interface{} {
	result := map[string]interface{}{
		"id":            inv.ID,
		"function_id":   inv.FunctionID,
		"tenant_id":     inv.TenantID,
		"deployment_id": inv.DeploymentID,
		"request_id":    inv.RequestID,
		"success":       inv.Success,
		"duration_ms":   inv.DurationMs,
		"started_at":    inv.StartedAt.UTC().Format(time.RFC3339),
		"completed_at":  inv.CompletedAt.UTC().Format(time.RFC3339),
	}
	if inv.TriggerID != nil {
		result["trigger_id"] = *inv.TriggerID
	}
	if inv.InputHash != nil {
		result["input_hash"] = *inv.InputHash
	}
	if inv.InputSize != nil {
		result["input_size"] = *inv.InputSize
	}
	if inv.OutputHash != nil {
		result["output_hash"] = *inv.OutputHash
	}
	if inv.OutputSize != nil {
		result["output_size"] = *inv.OutputSize
	}
	if inv.ErrorMessage != nil {
		result["error_message"] = *inv.ErrorMessage
	}
	if inv.MemoryBytes != nil {
		result["memory_bytes"] = *inv.MemoryBytes
	}
	if inv.CPUTimeMs != nil {
		result["cpu_time_ms"] = *inv.CPUTimeMs
	}
	return result
}

// handleTriggersRoot handles /api/v1/triggers (list all triggers for tenant).
func (a *FunctionAPI) handleTriggersRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		a.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "X-Tenant-ID header required",
		})
		return
	}

	limit := 50
	offset := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		if l, err := strconv.Atoi(v); err == nil && l > 0 && l <= 100 {
			limit = l
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if o, err := strconv.Atoi(v); err == nil && o >= 0 {
			offset = o
		}
	}

	triggers, err := a.repo.ListTriggersByTenant(r.Context(), tenantID, limit, offset)
	if err != nil {
		a.server.logger.Error("list triggers error", "error", err)
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "internal error",
		})
		return
	}

	result := make([]map[string]interface{}, 0, len(triggers))
	for _, t := range triggers {
		result = append(result, triggerToJSON(&t))
	}

	a.writeJSON(w, http.StatusOK, map[string]interface{}{
		"triggers": result,
		"count":    len(result),
		"limit":    limit,
		"offset":   offset,
	})
}

// handleTriggerByID handles /api/v1/triggers/{id} (get/update/delete trigger).
func (a *FunctionAPI) handleTriggerByID(w http.ResponseWriter, r *http.Request) {
	triggerID := strings.TrimPrefix(r.URL.Path, "/api/v1/triggers/")
	if triggerID == "" {
		http.Error(w, "trigger ID required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		a.getTrigger(w, r, triggerID)
	case http.MethodPut:
		a.updateTrigger(w, r, triggerID)
	case http.MethodDelete:
		a.deleteTrigger(w, r, triggerID)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// getTrigger retrieves a trigger by ID.
func (a *FunctionAPI) getTrigger(w http.ResponseWriter, r *http.Request, triggerID string) {
	t, err := a.repo.GetTrigger(r.Context(), triggerID)
	if err != nil {
		a.server.logger.Error("get trigger error", "error", err)
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "internal error",
		})
		return
	}

	if t == nil {
		a.writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"error": "trigger not found",
			"id":    triggerID,
		})
		return
	}

	a.writeJSON(w, http.StatusOK, triggerToJSON(t))
}

// updateTrigger updates a trigger.
func (a *FunctionAPI) updateTrigger(w http.ResponseWriter, r *http.Request, triggerID string) {
	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		a.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "X-Tenant-ID header required",
		})
		return
	}

	t, err := a.repo.GetTrigger(r.Context(), triggerID)
	if err != nil {
		a.server.logger.Error("get trigger error", "error", err)
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "internal error",
		})
		return
	}

	if t == nil {
		a.writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"error": "trigger not found",
			"id":    triggerID,
		})
		return
	}

	// Verify tenant ownership
	if t.TenantID != tenantID {
		a.writeJSON(w, http.StatusForbidden, map[string]interface{}{
			"error": "not authorized to update this trigger",
		})
		return
	}

	var body struct {
		Name          *string         `json:"name"`
		EventType     *string         `json:"event_type"`
		FilterChain   *int16          `json:"filter_chain"`
		FilterAddress *string         `json:"filter_address"`
		FilterTopic   *string         `json:"filter_topic"`
		FilterJSON    json.RawMessage `json:"filter_json"`
		Priority      *int            `json:"priority"`
		MaxRetries    *int            `json:"max_retries"`
		TimeoutMs     *int            `json:"timeout_ms"`
		Status        *string         `json:"status"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		a.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "invalid request body",
		})
		return
	}

	// Apply updates
	if body.Name != nil {
		t.Name = *body.Name
	}
	if body.EventType != nil {
		t.EventType = *body.EventType
	}
	if body.FilterChain != nil {
		t.FilterChain = body.FilterChain
	}
	if body.FilterAddress != nil {
		t.FilterAddress = body.FilterAddress
	}
	if body.FilterTopic != nil {
		t.FilterTopic = body.FilterTopic
	}
	if len(body.FilterJSON) > 0 {
		t.FilterJSON = body.FilterJSON
	}
	if body.Priority != nil {
		t.Priority = *body.Priority
	}
	if body.MaxRetries != nil {
		t.MaxRetries = *body.MaxRetries
	}
	if body.TimeoutMs != nil {
		t.TimeoutMs = *body.TimeoutMs
	}
	if body.Status != nil {
		switch *body.Status {
		case "active":
			t.Status = storage.TriggerStatusActive
		case "disabled":
			t.Status = storage.TriggerStatusDisabled
		default:
			a.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"error": "invalid status: must be 'active' or 'disabled'",
			})
			return
		}
	}

	if err := a.repo.UpdateTrigger(r.Context(), t); err != nil {
		a.server.logger.Error("update trigger error", "error", err)
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "failed to update trigger",
		})
		return
	}

	a.server.logger.Info("trigger updated", "trigger_id", triggerID)

	a.writeJSON(w, http.StatusOK, triggerToJSON(t))
}

// deleteTrigger deletes a trigger.
func (a *FunctionAPI) deleteTrigger(w http.ResponseWriter, r *http.Request, triggerID string) {
	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		a.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "X-Tenant-ID header required",
		})
		return
	}

	// Verify the trigger exists and belongs to the tenant
	t, err := a.repo.GetTrigger(r.Context(), triggerID)
	if err != nil {
		a.server.logger.Error("get trigger error", "error", err)
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "internal error",
		})
		return
	}

	if t == nil {
		a.writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"error": "trigger not found",
			"id":    triggerID,
		})
		return
	}

	// Verify tenant ownership
	if t.TenantID != tenantID {
		a.writeJSON(w, http.StatusForbidden, map[string]interface{}{
			"error": "not authorized to delete this trigger",
		})
		return
	}

	if err := a.repo.DeleteTrigger(r.Context(), triggerID); err != nil {
		if strings.Contains(err.Error(), "not found") {
			a.writeJSON(w, http.StatusNotFound, map[string]interface{}{
				"error": "trigger not found",
				"id":    triggerID,
			})
			return
		}
		a.server.logger.Error("delete trigger error", "error", err)
		a.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "failed to delete trigger",
		})
		return
	}

	a.server.logger.Info("trigger deleted", "trigger_id", triggerID)

	a.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "deleted",
		"id":     triggerID,
	})
}
