package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/marko911/project-pulse/internal/correctness"
	"github.com/marko911/project-pulse/internal/platform/storage"
	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

// Server holds API gateway dependencies.
type Server struct {
	cfg    Config
	logger *slog.Logger

	// Correctness components (injected or created lazily)
	watermark   *correctness.WatermarkController
	gapDetector *correctness.GapDetector
	repo        *storage.ManifestRepository

	// WebSocket handler for subscription management
	wsHandler *WebSocketHandler

	// Function API handler
	functionAPI *FunctionAPI
}

// NewServer creates a new API gateway server.
func NewServer(cfg Config, logger *slog.Logger) *Server {
	return &Server{
		cfg:    cfg,
		logger: logger.With("component", "api-gateway"),
	}
}

// SetWatermarkController sets the watermark controller for the API.
func (s *Server) SetWatermarkController(w *correctness.WatermarkController) {
	s.watermark = w
}

// SetGapDetector sets the gap detector for the API.
func (s *Server) SetGapDetector(g *correctness.GapDetector) {
	s.gapDetector = g
}

// SetManifestRepository sets the manifest repository for the API.
func (s *Server) SetManifestRepository(r *storage.ManifestRepository) {
	s.repo = r
}

// SetWebSocketHandler sets the WebSocket handler for subscription management.
func (s *Server) SetWebSocketHandler(ws *WebSocketHandler) {
	s.wsHandler = ws
}

// SetFunctionAPI sets the function API handler.
func (s *Server) SetFunctionAPI(api *FunctionAPI) {
	s.functionAPI = api
}

// Router returns the HTTP handler for the API gateway.
func (s *Server) Router() http.Handler {
	mux := http.NewServeMux()

	// Health & status
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)

	// Correctness status endpoints
	mux.HandleFunc("/api/v1/correctness/status", s.handleCorrectnessStatus)
	mux.HandleFunc("/api/v1/correctness/watermark", s.handleWatermark)
	mux.HandleFunc("/api/v1/correctness/halts", s.handleHalts)
	mux.HandleFunc("/api/v1/correctness/gaps", s.handleGaps)

	// Manifest endpoints
	mux.HandleFunc("/api/v1/manifests/", s.handleManifests)
	mux.HandleFunc("/api/v1/manifests/mismatches", s.handleMismatches)

	// WebSocket endpoint for real-time event subscriptions
	if s.wsHandler != nil {
		mux.HandleFunc("/ws", s.wsHandler.HandleConnect)
		mux.HandleFunc("/api/v1/ws", s.wsHandler.HandleConnect)
	}

	// Function registry endpoints
	if s.functionAPI != nil {
		s.functionAPI.RegisterRoutes(mux)
	}

	// Trigger management endpoints (standalone)
	if s.functionAPI != nil {
		mux.HandleFunc("/api/v1/triggers", s.functionAPI.handleTriggersRoot)
		mux.HandleFunc("/api/v1/triggers/", s.functionAPI.handleTriggerByID)
	}

	// Profiling endpoints (pprof) for performance analysis
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
	mux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))

	// Wrap with logging middleware
	return s.loggingMiddleware(mux)
}

// loggingMiddleware logs all requests.
func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap response writer to capture status code
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		s.logger.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", wrapped.statusCode,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// handleHealth returns basic health status.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

// handleReady returns readiness status based on correctness state.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ready := true
	reasons := []string{}

	// Check watermark halt status
	if s.watermark != nil && s.watermark.IsHalted() {
		ready = false
		reasons = append(reasons, "watermark_halted")
	}

	// Check gap detector halt status
	if s.gapDetector != nil && s.gapDetector.IsHalted() {
		ready = false
		reasons = append(reasons, "gap_detector_halted")
	}

	status := map[string]interface{}{
		"ready":     ready,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}

	if !ready {
		status["reasons"] = reasons
		s.writeJSON(w, http.StatusServiceUnavailable, status)
		return
	}

	s.writeJSON(w, http.StatusOK, status)
}

// handleCorrectnessStatus returns overall correctness system status.
func (s *Server) handleCorrectnessStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := map[string]interface{}{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}

	// Watermark status
	if s.watermark != nil {
		status["watermark"] = s.watermark.Stats()
	} else {
		status["watermark"] = map[string]interface{}{"available": false}
	}

	// Gap detector status
	if s.gapDetector != nil {
		status["gap_detector"] = s.gapDetector.Stats()
	} else {
		status["gap_detector"] = map[string]interface{}{"available": false}
	}

	s.writeJSON(w, http.StatusOK, status)
}

// handleWatermark returns watermark state for chains.
func (s *Server) handleWatermark(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.watermark == nil {
		s.writeJSON(w, http.StatusOK, map[string]interface{}{
			"available": false,
			"message":   "watermark controller not configured",
		})
		return
	}

	// Optional chain filter
	chainParam := r.URL.Query().Get("chain")

	stats := s.watermark.Stats()

	if chainParam != "" {
		// Filter to specific chain
		chains := stats["chains"].(map[string]interface{})
		if chainData, ok := chains[chainParam]; ok {
			s.writeJSON(w, http.StatusOK, map[string]interface{}{
				"chain": chainParam,
				"state": chainData,
			})
		} else {
			s.writeJSON(w, http.StatusNotFound, map[string]interface{}{
				"error": "chain not found",
				"chain": chainParam,
			})
		}
		return
	}

	s.writeJSON(w, http.StatusOK, stats)
}

// handleHalts returns current halt conditions.
func (s *Server) handleHalts(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.getHalts(w, r)
	case http.MethodDelete:
		s.resolveHalt(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) getHalts(w http.ResponseWriter, r *http.Request) {
	if s.watermark == nil {
		s.writeJSON(w, http.StatusOK, map[string]interface{}{
			"halted":     false,
			"conditions": []interface{}{},
		})
		return
	}

	conditions := s.watermark.GetHaltConditions()
	result := make([]map[string]interface{}, 0, len(conditions))

	for _, c := range conditions {
		result = append(result, map[string]interface{}{
			"source":       string(c.Source),
			"chain":        chainNameFromProto(c.Chain),
			"block_number": c.BlockNumber,
			"reason":       c.Reason,
			"detected_at":  c.DetectedAt.UTC().Format(time.RFC3339),
		})
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"halted":     s.watermark.IsHalted(),
		"conditions": result,
	})
}

func (s *Server) resolveHalt(w http.ResponseWriter, r *http.Request) {
	if s.watermark == nil {
		http.Error(w, "watermark controller not available", http.StatusServiceUnavailable)
		return
	}

	var body struct {
		Source     string `json:"source"`
		Resolution string `json:"resolution"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "invalid request body",
		})
		return
	}

	if body.Source == "" || body.Resolution == "" {
		s.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "source and resolution are required",
		})
		return
	}

	source := correctness.HaltSource(body.Source)
	if err := s.watermark.ResolveHalt(source, body.Resolution); err != nil {
		s.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error":  err.Error(),
			"source": body.Source,
		})
		return
	}

	s.logger.Info("halt resolved via API",
		"source", body.Source,
		"resolution", body.Resolution,
	)

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":     "resolved",
		"source":     body.Source,
		"resolution": body.Resolution,
	})
}

// handleGaps returns gap detector information.
func (s *Server) handleGaps(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.gapDetector == nil {
		s.writeJSON(w, http.StatusOK, map[string]interface{}{
			"available": false,
			"message":   "gap detector not configured",
		})
		return
	}

	s.writeJSON(w, http.StatusOK, s.gapDetector.Stats())
}

// handleManifests routes manifest requests.
func (s *Server) handleManifests(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse path: /api/v1/manifests/{chain}/{block}
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/manifests/")
	parts := strings.Split(path, "/")

	if len(parts) < 1 || parts[0] == "" {
		s.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "chain required",
			"usage": "/api/v1/manifests/{chain}/{block}",
		})
		return
	}

	chainName := parts[0]
	chain := protoChainFromName(chainName)
	if chain == protov1.Chain_CHAIN_UNSPECIFIED {
		s.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "invalid chain",
			"chain": chainName,
		})
		return
	}

	if s.repo == nil {
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"error": "manifest repository not available",
		})
		return
	}

	// If block number specified, get specific manifest
	if len(parts) >= 2 && parts[1] != "" {
		blockNum, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"error": "invalid block number",
				"block": parts[1],
			})
			return
		}

		s.getManifest(w, r, chain, blockNum)
		return
	}

	// Get range of manifests
	s.getManifestRange(w, r, chain)
}

func (s *Server) getManifest(w http.ResponseWriter, r *http.Request, chain protov1.Chain, blockNum uint64) {
	manifest, err := s.repo.GetByBlock(r.Context(), chain, blockNum)
	if err != nil {
		s.logger.Error("get manifest error", "error", err)
		s.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "internal error",
		})
		return
	}

	if manifest == nil {
		s.writeJSON(w, http.StatusNotFound, map[string]interface{}{
			"error":        "manifest not found",
			"chain":        chainNameFromProto(chain),
			"block_number": blockNum,
		})
		return
	}

	s.writeJSON(w, http.StatusOK, manifestToJSON(manifest))
}

func (s *Server) getManifestRange(w http.ResponseWriter, r *http.Request, chain protov1.Chain) {
	// Parse query params for range
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")
	limitStr := r.URL.Query().Get("limit")

	// Default limit
	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 1000 {
			limit = l
		}
	}

	// Get latest block if no range specified
	if fromStr == "" && toStr == "" {
		latest, err := s.repo.GetLatestBlock(r.Context(), chain)
		if err != nil {
			s.logger.Error("get latest block error", "error", err)
			s.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"error": "internal error",
			})
			return
		}

		if latest == 0 {
			s.writeJSON(w, http.StatusOK, map[string]interface{}{
				"chain":     chainNameFromProto(chain),
				"manifests": []interface{}{},
				"count":     0,
			})
			return
		}

		fromBlock := uint64(0)
		if latest > uint64(limit) {
			fromBlock = latest - uint64(limit)
		}

		manifests, err := s.repo.GetBlockRange(r.Context(), chain, fromBlock, latest)
		if err != nil {
			s.logger.Error("get block range error", "error", err)
			s.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"error": "internal error",
			})
			return
		}

		result := make([]map[string]interface{}, 0, len(manifests))
		for _, m := range manifests {
			result = append(result, manifestToJSON(&m))
		}

		s.writeJSON(w, http.StatusOK, map[string]interface{}{
			"chain":        chainNameFromProto(chain),
			"from":         fromBlock,
			"to":           latest,
			"manifests":    result,
			"count":        len(result),
			"latest_block": latest,
		})
		return
	}

	// Parse from/to
	var fromBlock, toBlock uint64
	var err error

	if fromStr != "" {
		fromBlock, err = strconv.ParseUint(fromStr, 10, 64)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"error": "invalid from parameter",
			})
			return
		}
	}

	if toStr != "" {
		toBlock, err = strconv.ParseUint(toStr, 10, 64)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"error": "invalid to parameter",
			})
			return
		}
	} else {
		toBlock = fromBlock + uint64(limit)
	}

	// Limit range
	if toBlock-fromBlock > 1000 {
		toBlock = fromBlock + 1000
	}

	manifests, err := s.repo.GetBlockRange(r.Context(), chain, fromBlock, toBlock)
	if err != nil {
		s.logger.Error("get block range error", "error", err)
		s.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "internal error",
		})
		return
	}

	result := make([]map[string]interface{}, 0, len(manifests))
	for _, m := range manifests {
		result = append(result, manifestToJSON(&m))
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"chain":     chainNameFromProto(chain),
		"from":      fromBlock,
		"to":        toBlock,
		"manifests": result,
		"count":     len(result),
	})
}

// handleMismatches returns manifests with count mismatches.
func (s *Server) handleMismatches(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	chainName := r.URL.Query().Get("chain")
	if chainName == "" {
		s.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "chain parameter required",
		})
		return
	}

	chain := protoChainFromName(chainName)
	if chain == protov1.Chain_CHAIN_UNSPECIFIED {
		s.writeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "invalid chain",
			"chain": chainName,
		})
		return
	}

	if s.repo == nil {
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"error": "manifest repository not available",
		})
		return
	}

	limit := 100
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 1000 {
			limit = l
		}
	}

	manifests, err := s.repo.FindMismatches(r.Context(), chain, limit)
	if err != nil {
		s.logger.Error("find mismatches error", "error", err)
		s.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": "internal error",
		})
		return
	}

	result := make([]map[string]interface{}, 0, len(manifests))
	for _, m := range manifests {
		result = append(result, manifestToJSON(&m))
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"chain":      chainName,
		"mismatches": result,
		"count":      len(result),
	})
}

// writeJSON writes a JSON response.
func (s *Server) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		s.logger.Error("JSON encode error", "error", err)
	}
}

// manifestToJSON converts a ManifestRecord to JSON-friendly map.
func manifestToJSON(m *storage.ManifestRecord) map[string]interface{} {
	return map[string]interface{}{
		"id":                   m.ID,
		"chain":                chainNameFromProto(protov1.Chain(m.Chain)),
		"block_number":         m.BlockNumber,
		"block_hash":           m.BlockHash,
		"parent_hash":          m.ParentHash,
		"expected_tx_count":    m.ExpectedTxCount,
		"expected_event_count": m.ExpectedEventCount,
		"emitted_tx_count":     m.EmittedTxCount,
		"emitted_event_count":  m.EmittedEventCount,
		"event_ids_hash":       m.EventIdsHash,
		"block_timestamp":      m.BlockTimestamp.UTC().Format(time.RFC3339),
		"ingested_at":          m.IngestedAt.UTC().Format(time.RFC3339),
		"manifest_created_at":  m.ManifestCreatedAt.UTC().Format(time.RFC3339),
		"matched":              m.ExpectedTxCount == m.EmittedTxCount && m.ExpectedEventCount == m.EmittedEventCount,
	}
}

// chainNameFromProto converts a protov1.Chain to a string name.
func chainNameFromProto(chain protov1.Chain) string {
	switch chain {
	case protov1.Chain_CHAIN_ETHEREUM:
		return "ethereum"
	case protov1.Chain_CHAIN_SOLANA:
		return "solana"
	case protov1.Chain_CHAIN_POLYGON:
		return "polygon"
	case protov1.Chain_CHAIN_ARBITRUM:
		return "arbitrum"
	case protov1.Chain_CHAIN_OPTIMISM:
		return "optimism"
	case protov1.Chain_CHAIN_BASE:
		return "base"
	case protov1.Chain_CHAIN_AVALANCHE:
		return "avalanche"
	case protov1.Chain_CHAIN_BSC:
		return "bsc"
	default:
		return "unknown"
	}
}

// protoChainFromName converts a chain name string to protov1.Chain.
func protoChainFromName(name string) protov1.Chain {
	switch strings.ToLower(name) {
	case "ethereum", "eth":
		return protov1.Chain_CHAIN_ETHEREUM
	case "solana", "sol":
		return protov1.Chain_CHAIN_SOLANA
	case "polygon", "matic":
		return protov1.Chain_CHAIN_POLYGON
	case "arbitrum", "arb":
		return protov1.Chain_CHAIN_ARBITRUM
	case "optimism", "op":
		return protov1.Chain_CHAIN_OPTIMISM
	case "base":
		return protov1.Chain_CHAIN_BASE
	case "avalanche", "avax":
		return protov1.Chain_CHAIN_AVALANCHE
	case "bsc", "bnb":
		return protov1.Chain_CHAIN_BSC
	default:
		return protov1.Chain_CHAIN_UNSPECIFIED
	}
}

// HandleNATSEvent routes a canonical event received from NATS to connected WebSocket clients.
// This is called by the NATS consumer when events are received from JetStream.
func (s *Server) HandleNATSEvent(event *protov1.CanonicalEvent) {
	if s.wsHandler == nil {
		return
	}

	// Log event receipt for debugging/monitoring
	s.logger.Debug("received NATS event",
		"event_id", event.EventId,
		"chain", chainNameFromProto(event.Chain),
		"event_type", event.EventType,
		"block_number", event.BlockNumber,
	)

	// Route the event to subscribed WebSocket clients
	s.wsHandler.RouteEvent(event)
}
