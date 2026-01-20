package websocket

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/marko911/project-pulse/internal/delivery/routing"
	"github.com/marko911/project-pulse/internal/delivery/subscription"
)

// Manager manages WebSocket connections and their subscriptions.
// It implements routing.DestinationRegistry for integration with the event router.
type Manager struct {
	mu           sync.RWMutex
	destinations map[string]*Destination
	subs         subscription.Manager
	logger       *slog.Logger

	// Metrics
	totalConnections   int64
	activeConnections  int64
	messagesDelivered  int64
	connectionErrors   int64
}

// ManagerConfig holds configuration for the WebSocket manager.
type ManagerConfig struct {
	// SubscriptionManager for managing client subscriptions.
	SubscriptionManager subscription.Manager

	// Logger for connection events.
	Logger *slog.Logger
}

// NewManager creates a new WebSocket connection manager.
func NewManager(cfg ManagerConfig) *Manager {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Manager{
		destinations: make(map[string]*Destination),
		subs:         cfg.SubscriptionManager,
		logger:       cfg.Logger.With("component", "websocket-manager"),
	}
}

// HandleConnection upgrades an HTTP connection to WebSocket and manages it.
func (m *Manager) HandleConnection(ctx context.Context, clientID string, conn *websocket.Conn, metadata map[string]string) *Destination {
	dest := NewDestination(DestinationConfig{
		ID:             clientID,
		Conn:           conn,
		SendBufferSize: 256,
		Metadata:       metadata,
		OnClose:        m.handleDisconnect,
	})

	m.Register(clientID, dest)

	m.logger.Info("client connected",
		"client_id", clientID,
		"remote_addr", conn.RemoteAddr().String(),
	)

	return dest
}

// handleDisconnect cleans up when a client disconnects.
// Note: This is called from the Destination's Close method, which may already
// hold locks. We use a separate goroutine for cleanup to avoid deadlocks.
func (m *Manager) handleDisconnect(clientID string) {
	// Schedule cleanup in separate goroutine to avoid deadlock
	go func() {
		m.mu.Lock()
		delete(m.destinations, clientID)
		m.activeConnections--
		m.mu.Unlock()

		// Clean up subscriptions for this client
		if m.subs != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := m.subs.UnsubscribeAll(ctx, clientID); err != nil {
				m.logger.Error("failed to cleanup subscriptions",
					"client_id", clientID,
					"error", err,
				)
			}
		}

		m.logger.Info("client disconnected", "client_id", clientID)
	}()
}

// Get retrieves a destination by client ID.
// Implements routing.DestinationRegistry.
func (m *Manager) Get(clientID string) (routing.Destination, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	dest, ok := m.destinations[clientID]
	if !ok || dest.IsClosed() {
		return nil, false
	}
	return dest, true
}

// Register adds a destination for a client.
// Implements routing.DestinationRegistry.
func (m *Manager) Register(clientID string, dest routing.Destination) {
	m.mu.Lock()
	// Get existing connection to close later
	existing, hasExisting := m.destinations[clientID]

	if d, ok := dest.(*Destination); ok {
		m.destinations[clientID] = d
		m.totalConnections++
		if !hasExisting {
			m.activeConnections++
		}
	}
	m.mu.Unlock()

	// Close existing connection outside of lock
	// Clear its onClose callback to prevent it from removing the new connection
	if hasExisting && existing != nil {
		existing.onClose = nil // Prevent callback from cleaning up new registration
		existing.Close()
	}
}

// Unregister removes a destination.
// Implements routing.DestinationRegistry.
func (m *Manager) Unregister(clientID string) {
	m.mu.Lock()
	dest, ok := m.destinations[clientID]
	if ok {
		delete(m.destinations, clientID)
		m.activeConnections--
	}
	m.mu.Unlock()

	// Close outside of lock to avoid deadlock with handleDisconnect
	if ok && dest != nil {
		dest.Close()
	}
}

// All returns all registered destinations.
// Implements routing.DestinationRegistry.
func (m *Manager) All() map[string]routing.Destination {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]routing.Destination, len(m.destinations))
	for k, v := range m.destinations {
		if !v.IsClosed() {
			result[k] = v
		}
	}
	return result
}

// ActiveCount returns the number of active connections.
func (m *Manager) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.destinations)
}

// Stats returns manager statistics.
func (m *Manager) Stats() ManagerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return ManagerStats{
		TotalConnections:  m.totalConnections,
		ActiveConnections: int64(len(m.destinations)),
		MessagesDelivered: m.messagesDelivered,
		ConnectionErrors:  m.connectionErrors,
	}
}

// ManagerStats contains WebSocket manager statistics.
type ManagerStats struct {
	TotalConnections  int64 `json:"total_connections"`
	ActiveConnections int64 `json:"active_connections"`
	MessagesDelivered int64 `json:"messages_delivered"`
	ConnectionErrors  int64 `json:"connection_errors"`
}

// Subscribe creates a subscription for a client.
func (m *Manager) Subscribe(ctx context.Context, clientID string, filter subscription.Filter) (string, error) {
	if m.subs == nil {
		return "", nil
	}

	sub := &subscription.Subscription{
		ClientID:  clientID,
		Filter:    filter,
		ExpiresAt: time.Now().Add(24 * time.Hour), // Default 24h TTL
	}

	return m.subs.Subscribe(ctx, sub)
}

// Unsubscribe removes a subscription.
func (m *Manager) Unsubscribe(ctx context.Context, subID string) error {
	if m.subs == nil {
		return nil
	}
	return m.subs.Unsubscribe(ctx, subID)
}

// GetClientSubscriptions returns all subscriptions for a client.
func (m *Manager) GetClientSubscriptions(ctx context.Context, clientID string) ([]*subscription.Subscription, error) {
	if m.subs == nil {
		return nil, nil
	}
	return m.subs.ListByClient(ctx, clientID)
}

// RefreshSubscription extends the TTL of a subscription.
func (m *Manager) RefreshSubscription(ctx context.Context, subID string, ttl time.Duration) error {
	if m.subs == nil {
		return nil
	}
	return m.subs.Refresh(ctx, subID, time.Now().Add(ttl))
}

// Broadcast sends a message to all connected clients.
func (m *Manager) Broadcast(message []byte) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, dest := range m.destinations {
		if !dest.IsClosed() {
			select {
			case dest.send <- message:
			default:
				// Buffer full, skip
			}
		}
	}
}

// Close shuts down all connections.
func (m *Manager) Close() error {
	m.mu.Lock()
	dests := make([]*Destination, 0, len(m.destinations))
	for _, dest := range m.destinations {
		dests = append(dests, dest)
	}
	m.destinations = make(map[string]*Destination)
	m.mu.Unlock()

	// Close outside of lock to avoid deadlock with handleDisconnect
	for _, dest := range dests {
		dest.Close()
	}

	return nil
}

// Ensure Manager implements routing.DestinationRegistry
var _ routing.DestinationRegistry = (*Manager)(nil)
