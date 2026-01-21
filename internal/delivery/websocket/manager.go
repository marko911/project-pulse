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

type Manager struct {
	mu           sync.RWMutex
	destinations map[string]*Destination
	subs         subscription.Manager
	logger       *slog.Logger

	totalConnections   int64
	activeConnections  int64
	messagesDelivered  int64
	connectionErrors   int64
}

type ManagerConfig struct {
	SubscriptionManager subscription.Manager

	Logger *slog.Logger
}

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

func (m *Manager) handleDisconnect(clientID string) {
	go func() {
		m.mu.Lock()
		delete(m.destinations, clientID)
		m.activeConnections--
		m.mu.Unlock()

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

func (m *Manager) Get(clientID string) (routing.Destination, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	dest, ok := m.destinations[clientID]
	if !ok || dest.IsClosed() {
		return nil, false
	}
	return dest, true
}

func (m *Manager) Register(clientID string, dest routing.Destination) {
	m.mu.Lock()
	existing, hasExisting := m.destinations[clientID]

	if d, ok := dest.(*Destination); ok {
		m.destinations[clientID] = d
		m.totalConnections++
		if !hasExisting {
			m.activeConnections++
		}
	}
	m.mu.Unlock()

	if hasExisting && existing != nil {
		existing.onClose = nil
		existing.Close()
	}
}

func (m *Manager) Unregister(clientID string) {
	m.mu.Lock()
	dest, ok := m.destinations[clientID]
	if ok {
		delete(m.destinations, clientID)
		m.activeConnections--
	}
	m.mu.Unlock()

	if ok && dest != nil {
		dest.Close()
	}
}

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

func (m *Manager) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.destinations)
}

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

type ManagerStats struct {
	TotalConnections  int64 `json:"total_connections"`
	ActiveConnections int64 `json:"active_connections"`
	MessagesDelivered int64 `json:"messages_delivered"`
	ConnectionErrors  int64 `json:"connection_errors"`
}

func (m *Manager) Subscribe(ctx context.Context, clientID string, filter subscription.Filter) (string, error) {
	if m.subs == nil {
		return "", nil
	}

	sub := &subscription.Subscription{
		ClientID:  clientID,
		Filter:    filter,
		ExpiresAt: time.Now().Add(24 * time.Hour),
	}

	return m.subs.Subscribe(ctx, sub)
}

func (m *Manager) Unsubscribe(ctx context.Context, subID string) error {
	if m.subs == nil {
		return nil
	}
	return m.subs.Unsubscribe(ctx, subID)
}

func (m *Manager) GetClientSubscriptions(ctx context.Context, clientID string) ([]*subscription.Subscription, error) {
	if m.subs == nil {
		return nil, nil
	}
	return m.subs.ListByClient(ctx, clientID)
}

func (m *Manager) RefreshSubscription(ctx context.Context, subID string, ttl time.Duration) error {
	if m.subs == nil {
		return nil
	}
	return m.subs.Refresh(ctx, subID, time.Now().Add(ttl))
}

func (m *Manager) Broadcast(message []byte) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, dest := range m.destinations {
		if !dest.IsClosed() {
			select {
			case dest.send <- message:
			default:
			}
		}
	}
}

func (m *Manager) Close() error {
	m.mu.Lock()
	dests := make([]*Destination, 0, len(m.destinations))
	for _, dest := range m.destinations {
		dests = append(dests, dest)
	}
	m.destinations = make(map[string]*Destination)
	m.mu.Unlock()

	for _, dest := range dests {
		dest.Close()
	}

	return nil
}

var _ routing.DestinationRegistry = (*Manager)(nil)
