package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/marko911/project-pulse/internal/delivery/subscription"
	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

// WebSocketHandler handles WebSocket connections and subscription management.
type WebSocketHandler struct {
	subManager     *subscription.RedisManager
	logger         *slog.Logger
	allowedOrigins []string // nil means allow all origins
	upgrader       websocket.Upgrader

	// Active connections
	mu          sync.RWMutex
	connections map[string]*wsConnection
}

type wsConnection struct {
	clientID string
	conn     *websocket.Conn
	send     chan []byte
	closed   bool
	mu       sync.Mutex
}

// NewWebSocketHandler creates a new WebSocket handler.
// allowedOrigins specifies which origins are allowed to connect via WebSocket.
// If nil or empty, all origins are allowed.
func NewWebSocketHandler(subManager *subscription.RedisManager, allowedOrigins []string, logger *slog.Logger) *WebSocketHandler {
	h := &WebSocketHandler{
		subManager:     subManager,
		allowedOrigins: allowedOrigins,
		logger:         logger.With("component", "websocket"),
		connections:    make(map[string]*wsConnection),
	}

	// Configure upgrader with origin check
	h.upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     h.checkOrigin,
	}

	return h
}

// checkOrigin validates the request origin against allowed origins.
func (h *WebSocketHandler) checkOrigin(r *http.Request) bool {
	// If no allowed origins configured, allow all
	if len(h.allowedOrigins) == 0 {
		return true
	}

	origin := r.Header.Get("Origin")
	if origin == "" {
		// No origin header - allow (same-origin requests may not have it)
		return true
	}

	// Check against allowed origins
	for _, allowed := range h.allowedOrigins {
		if allowed == "*" {
			return true
		}
		// Case-insensitive match
		if strings.EqualFold(origin, allowed) {
			return true
		}
		// Wildcard subdomain match (e.g., "*.example.com")
		if strings.HasPrefix(allowed, "*.") {
			suffix := allowed[1:] // ".example.com"
			if strings.HasSuffix(strings.ToLower(origin), strings.ToLower(suffix)) {
				return true
			}
		}
	}

	h.logger.Warn("websocket connection rejected: origin not allowed",
		"origin", origin,
		"allowed_origins", h.allowedOrigins,
	)
	return false
}

// HandleConnect upgrades HTTP to WebSocket and manages the connection.
func (h *WebSocketHandler) HandleConnect(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("websocket upgrade failed", "error", err)
		return
	}

	// Generate unique client ID
	clientID := generateClientID()

	wsc := &wsConnection{
		clientID: clientID,
		conn:     conn,
		send:     make(chan []byte, 256),
	}

	// Register connection
	h.mu.Lock()
	h.connections[clientID] = wsc
	h.mu.Unlock()

	h.logger.Info("websocket connected", "client_id", clientID)

	// Send welcome message
	welcome := map[string]interface{}{
		"type":      "connected",
		"client_id": clientID,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	if data, err := json.Marshal(welcome); err == nil {
		wsc.send <- data
	}

	// Start read/write goroutines
	go h.readPump(wsc)
	go h.writePump(wsc)
}

// readPump reads messages from the WebSocket connection.
func (h *WebSocketHandler) readPump(wsc *wsConnection) {
	defer func() {
		h.closeConnection(wsc)
	}()

	wsc.conn.SetReadLimit(64 * 1024) // 64KB max message size
	wsc.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	wsc.conn.SetPongHandler(func(string) error {
		wsc.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, message, err := wsc.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				h.logger.Error("websocket read error", "client_id", wsc.clientID, "error", err)
			}
			break
		}

		h.handleMessage(wsc, message)
	}
}

// writePump writes messages to the WebSocket connection.
func (h *WebSocketHandler) writePump(wsc *wsConnection) {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		wsc.conn.Close()
	}()

	for {
		select {
		case message, ok := <-wsc.send:
			wsc.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				wsc.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := wsc.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

		case <-ticker.C:
			wsc.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := wsc.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// handleMessage processes incoming WebSocket messages.
func (h *WebSocketHandler) handleMessage(wsc *wsConnection, message []byte) {
	var msg struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}

	if err := json.Unmarshal(message, &msg); err != nil {
		h.sendError(wsc, "invalid_message", "failed to parse message")
		return
	}

	switch msg.Type {
	case "subscribe":
		h.handleSubscribe(wsc, msg.Data)
	case "unsubscribe":
		h.handleUnsubscribe(wsc, msg.Data)
	case "list_subscriptions":
		h.handleListSubscriptions(wsc)
	case "ping":
		h.sendMessage(wsc, "pong", nil)
	default:
		h.sendError(wsc, "unknown_type", "unknown message type: "+msg.Type)
	}
}

// handleSubscribe creates a new subscription for the client.
func (h *WebSocketHandler) handleSubscribe(wsc *wsConnection, data json.RawMessage) {
	var req struct {
		Chains          []string `json:"chains"`
		EventTypes      []string `json:"event_types"`
		Accounts        []string `json:"accounts"`
		ProgramIds      []string `json:"program_ids"`
		CommitmentLevel string   `json:"commitment_level"`
		TTLSeconds      int      `json:"ttl_seconds"`
	}

	if err := json.Unmarshal(data, &req); err != nil {
		h.sendError(wsc, "invalid_data", "failed to parse subscription data")
		return
	}

	// Convert chain names to protov1.Chain
	chains := make([]protov1.Chain, 0, len(req.Chains))
	for _, name := range req.Chains {
		chain := protoChainFromName(name)
		if chain != protov1.Chain_CHAIN_UNSPECIFIED {
			chains = append(chains, chain)
		}
	}

	// Build subscription
	sub := &subscription.Subscription{
		ClientID: wsc.clientID,
		Filter: subscription.Filter{
			Chains:     chains,
			EventTypes: req.EventTypes,
			Accounts:   req.Accounts,
			ProgramIds: req.ProgramIds,
		},
	}

	// Set TTL
	if req.TTLSeconds > 0 {
		sub.ExpiresAt = time.Now().Add(time.Duration(req.TTLSeconds) * time.Second)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	subID, err := h.subManager.Subscribe(ctx, sub)
	if err != nil {
		h.logger.Error("subscribe failed", "client_id", wsc.clientID, "error", err)
		h.sendError(wsc, "subscribe_failed", err.Error())
		return
	}

	h.logger.Info("subscription created",
		"client_id", wsc.clientID,
		"subscription_id", subID,
		"chains", req.Chains,
		"event_types", req.EventTypes,
	)

	h.sendMessage(wsc, "subscribed", map[string]interface{}{
		"subscription_id": subID,
		"filter": map[string]interface{}{
			"chains":      req.Chains,
			"event_types": req.EventTypes,
			"accounts":    req.Accounts,
			"program_ids": req.ProgramIds,
		},
	})
}

// handleUnsubscribe removes a subscription.
func (h *WebSocketHandler) handleUnsubscribe(wsc *wsConnection, data json.RawMessage) {
	var req struct {
		SubscriptionID string `json:"subscription_id"`
	}

	if err := json.Unmarshal(data, &req); err != nil {
		h.sendError(wsc, "invalid_data", "failed to parse unsubscribe data")
		return
	}

	if req.SubscriptionID == "" {
		h.sendError(wsc, "missing_id", "subscription_id is required")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := h.subManager.Unsubscribe(ctx, req.SubscriptionID); err != nil {
		h.logger.Error("unsubscribe failed", "subscription_id", req.SubscriptionID, "error", err)
		h.sendError(wsc, "unsubscribe_failed", err.Error())
		return
	}

	h.logger.Info("subscription removed",
		"client_id", wsc.clientID,
		"subscription_id", req.SubscriptionID,
	)

	h.sendMessage(wsc, "unsubscribed", map[string]interface{}{
		"subscription_id": req.SubscriptionID,
	})
}

// handleListSubscriptions returns all subscriptions for the client.
func (h *WebSocketHandler) handleListSubscriptions(wsc *wsConnection) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	subs, err := h.subManager.ListByClient(ctx, wsc.clientID)
	if err != nil {
		h.logger.Error("list subscriptions failed", "client_id", wsc.clientID, "error", err)
		h.sendError(wsc, "list_failed", err.Error())
		return
	}

	result := make([]map[string]interface{}, 0, len(subs))
	for _, sub := range subs {
		result = append(result, map[string]interface{}{
			"id":         sub.ID,
			"created_at": sub.CreatedAt.UTC().Format(time.RFC3339),
			"expires_at": sub.ExpiresAt.UTC().Format(time.RFC3339),
			"filter": map[string]interface{}{
				"chains":      chainNamesFromProtos(sub.Filter.Chains),
				"event_types": sub.Filter.EventTypes,
				"accounts":    sub.Filter.Accounts,
				"program_ids": sub.Filter.ProgramIds,
			},
		})
	}

	h.sendMessage(wsc, "subscriptions", map[string]interface{}{
		"subscriptions": result,
		"count":         len(result),
	})
}

// RouteEvent routes an event to all clients with matching subscriptions.
// Called by the NATS consumer when events are received from JetStream.
func (h *WebSocketHandler) RouteEvent(event *protov1.CanonicalEvent) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Find all subscriptions that match this event
	matches, err := h.subManager.Match(ctx, event)
	if err != nil {
		h.logger.Error("failed to find matching subscribers", "error", err)
		return
	}

	if len(matches) == 0 {
		return
	}

	// Send the event to each matching client
	for _, match := range matches {
		if err := h.SendEvent(match.ClientID, event); err != nil {
			h.logger.Warn("failed to send event to client",
				"client_id", match.ClientID,
				"event_id", event.EventId,
				"error", err,
			)
		}
	}

	h.logger.Debug("routed event to subscribers",
		"event_id", event.EventId,
		"subscriber_count", len(matches),
	)
}

// SendEvent sends an event to a specific client.
func (h *WebSocketHandler) SendEvent(clientID string, event *protov1.CanonicalEvent) error {
	h.mu.RLock()
	wsc, ok := h.connections[clientID]
	h.mu.RUnlock()

	if !ok {
		return nil // Connection no longer exists
	}

	data, err := json.Marshal(map[string]interface{}{
		"type": "event",
		"data": map[string]interface{}{
			"event_id":    event.EventId,
			"chain":       chainNameFromProto(event.Chain),
			"block_num":   event.BlockNumber,
			"tx_hash":     event.TxHash,
			"event_type":  event.EventType,
			"accounts":    event.Accounts,
			"program_id":  event.ProgramId,
			"timestamp":   event.Timestamp.UTC().Format(time.RFC3339),
			"native_value": event.NativeValue,
		},
	})
	if err != nil {
		return err
	}

	// Non-blocking send
	select {
	case wsc.send <- data:
	default:
		h.logger.Warn("event send channel full", "client_id", clientID)
	}

	return nil
}

// closeConnection cleans up when a connection closes.
func (h *WebSocketHandler) closeConnection(wsc *wsConnection) {
	wsc.mu.Lock()
	if wsc.closed {
		wsc.mu.Unlock()
		return
	}
	wsc.closed = true
	wsc.mu.Unlock()

	// Remove from connections map
	h.mu.Lock()
	delete(h.connections, wsc.clientID)
	h.mu.Unlock()

	// Clean up subscriptions
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := h.subManager.UnsubscribeAll(ctx, wsc.clientID); err != nil {
		h.logger.Error("cleanup subscriptions failed", "client_id", wsc.clientID, "error", err)
	}

	close(wsc.send)
	wsc.conn.Close()

	h.logger.Info("websocket disconnected", "client_id", wsc.clientID)
}

// sendMessage sends a typed message to the client.
func (h *WebSocketHandler) sendMessage(wsc *wsConnection, msgType string, data interface{}) {
	msg := map[string]interface{}{
		"type":      msgType,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	if data != nil {
		msg["data"] = data
	}

	jsonData, err := json.Marshal(msg)
	if err != nil {
		h.logger.Error("message marshal failed", "error", err)
		return
	}

	select {
	case wsc.send <- jsonData:
	default:
		h.logger.Warn("send channel full", "client_id", wsc.clientID)
	}
}

// sendError sends an error message to the client.
func (h *WebSocketHandler) sendError(wsc *wsConnection, code string, message string) {
	h.sendMessage(wsc, "error", map[string]interface{}{
		"code":    code,
		"message": message,
	})
}

// ConnectionCount returns the number of active connections.
func (h *WebSocketHandler) ConnectionCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.connections)
}

// generateClientID creates a unique client identifier.
func generateClientID() string {
	return time.Now().Format("20060102150405") + "_" + randomString(8)
}

// randomString generates a random alphanumeric string.
func randomString(n int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[time.Now().UnixNano()%int64(len(chars))]
		time.Sleep(time.Nanosecond)
	}
	return string(b)
}

// protoCommitmentFromName converts a commitment level name to proto enum.
func protoCommitmentFromName(name string) protov1.CommitmentLevel {
	switch name {
	case "processed":
		return protov1.CommitmentLevel_COMMITMENT_LEVEL_PROCESSED
	case "confirmed":
		return protov1.CommitmentLevel_COMMITMENT_LEVEL_CONFIRMED
	case "finalized":
		return protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED
	default:
		return protov1.CommitmentLevel_COMMITMENT_LEVEL_UNSPECIFIED
	}
}

// chainNamesFromProtos converts proto chains to name strings.
func chainNamesFromProtos(chains []protov1.Chain) []string {
	names := make([]string, 0, len(chains))
	for _, chain := range chains {
		names = append(names, chainNameFromProto(chain))
	}
	return names
}
