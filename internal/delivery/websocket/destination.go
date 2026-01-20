// Package websocket provides WebSocket-based event delivery for real-time subscriptions.
package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 512 * 1024 // 512KB
)

// Destination wraps a WebSocket connection for event delivery.
// Implements routing.Destination interface.
type Destination struct {
	id       string
	conn     *websocket.Conn
	send     chan []byte
	done     chan struct{}
	mu       sync.RWMutex
	closed   bool
	metadata map[string]string

	// Callbacks
	onClose func(id string)
}

// DestinationConfig holds configuration for a WebSocket destination.
type DestinationConfig struct {
	// ID is the unique identifier for this connection/client.
	ID string

	// Conn is the underlying WebSocket connection.
	Conn *websocket.Conn

	// SendBufferSize is the channel buffer size for outgoing messages.
	SendBufferSize int

	// Metadata holds optional client metadata.
	Metadata map[string]string

	// OnClose is called when the connection closes.
	OnClose func(id string)
}

// NewDestination creates a new WebSocket destination.
func NewDestination(cfg DestinationConfig) *Destination {
	if cfg.SendBufferSize <= 0 {
		cfg.SendBufferSize = 256
	}

	d := &Destination{
		id:       cfg.ID,
		conn:     cfg.Conn,
		send:     make(chan []byte, cfg.SendBufferSize),
		done:     make(chan struct{}),
		metadata: cfg.Metadata,
		onClose:  cfg.OnClose,
	}

	return d
}

// ID returns the unique identifier for this destination.
func (d *Destination) ID() string {
	return d.id
}

// Metadata returns the client metadata.
func (d *Destination) Metadata() map[string]string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.metadata
}

// Send delivers an event to the WebSocket client.
func (d *Destination) Send(ctx context.Context, event *protov1.CanonicalEvent) error {
	d.mu.RLock()
	if d.closed {
		d.mu.RUnlock()
		return fmt.Errorf("destination closed")
	}
	d.mu.RUnlock()

	msg, err := d.marshalEvent(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	select {
	case d.send <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-d.done:
		return fmt.Errorf("destination closed")
	default:
		// Buffer full, drop message
		return fmt.Errorf("send buffer full for client %s", d.id)
	}
}

// SendBatch delivers multiple events to the WebSocket client.
func (d *Destination) SendBatch(ctx context.Context, events []*protov1.CanonicalEvent) error {
	d.mu.RLock()
	if d.closed {
		d.mu.RUnlock()
		return fmt.Errorf("destination closed")
	}
	d.mu.RUnlock()

	// Marshal as batch message
	msg, err := d.marshalBatch(events)
	if err != nil {
		return fmt.Errorf("marshal batch: %w", err)
	}

	select {
	case d.send <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-d.done:
		return fmt.Errorf("destination closed")
	default:
		return fmt.Errorf("send buffer full for client %s", d.id)
	}
}

// Close releases resources associated with the destination.
func (d *Destination) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.mu.Unlock()

	close(d.done)

	if d.onClose != nil {
		d.onClose(d.id)
	}

	return d.conn.Close()
}

// IsClosed returns whether the destination has been closed.
func (d *Destination) IsClosed() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.closed
}

// Run starts the read and write pumps for the WebSocket connection.
// This should be called in a goroutine after creating the destination.
func (d *Destination) Run(ctx context.Context) {
	go d.writePump(ctx)
	d.readPump(ctx)
}

// readPump handles incoming messages from the WebSocket connection.
func (d *Destination) readPump(ctx context.Context) {
	defer d.Close()

	d.conn.SetReadLimit(maxMessageSize)
	d.conn.SetReadDeadline(time.Now().Add(pongWait))
	d.conn.SetPongHandler(func(string) error {
		d.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.done:
			return
		default:
		}

		_, message, err := d.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				// Log unexpected close
			}
			return
		}

		// Handle incoming message (subscription commands, pings, etc.)
		d.handleMessage(message)
	}
}

// writePump handles outgoing messages to the WebSocket connection.
func (d *Destination) writePump(ctx context.Context) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		d.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.done:
			return

		case message, ok := <-d.send:
			d.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// Channel closed
				d.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := d.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// Write queued messages
			n := len(d.send)
			for i := 0; i < n; i++ {
				w.Write([]byte{'\n'})
				w.Write(<-d.send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			d.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := d.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// handleMessage processes an incoming WebSocket message.
func (d *Destination) handleMessage(message []byte) {
	// Parse incoming message - could be subscription commands, heartbeats, etc.
	// For now, just acknowledge receipt
	var msg ClientMessage
	if err := json.Unmarshal(message, &msg); err != nil {
		return
	}

	// Handle different message types
	switch msg.Type {
	case "ping":
		// Respond with pong
		d.sendControlMessage("pong", nil)
	case "heartbeat":
		// Client heartbeat - connection is alive
	}
}

// sendControlMessage sends a control message to the client.
func (d *Destination) sendControlMessage(msgType string, data interface{}) {
	msg := ServerMessage{
		Type:      msgType,
		Timestamp: time.Now().UTC(),
		Data:      data,
	}
	if bytes, err := json.Marshal(msg); err == nil {
		select {
		case d.send <- bytes:
		default:
		}
	}
}

// marshalEvent converts a canonical event to JSON for WebSocket transmission.
func (d *Destination) marshalEvent(event *protov1.CanonicalEvent) ([]byte, error) {
	msg := ServerMessage{
		Type:      "event",
		Timestamp: time.Now().UTC(),
		Data:      eventToJSON(event),
	}
	return json.Marshal(msg)
}

// marshalBatch converts multiple events to a batch JSON message.
func (d *Destination) marshalBatch(events []*protov1.CanonicalEvent) ([]byte, error) {
	eventData := make([]map[string]interface{}, len(events))
	for i, e := range events {
		eventData[i] = eventToJSON(e)
	}

	msg := ServerMessage{
		Type:      "events",
		Timestamp: time.Now().UTC(),
		Data: map[string]interface{}{
			"count":  len(events),
			"events": eventData,
		},
	}
	return json.Marshal(msg)
}

// ClientMessage represents an incoming message from a WebSocket client.
type ClientMessage struct {
	Type string          `json:"type"`
	ID   string          `json:"id,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

// ServerMessage represents an outgoing message to a WebSocket client.
type ServerMessage struct {
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Data      interface{} `json:"data,omitempty"`
	Error     string      `json:"error,omitempty"`
}

// eventToJSON converts a CanonicalEvent to a JSON-friendly map.
func eventToJSON(event *protov1.CanonicalEvent) map[string]interface{} {
	return map[string]interface{}{
		"event_id":         event.EventId,
		"chain":            chainToString(event.Chain),
		"block_number":     event.BlockNumber,
		"block_hash":       event.BlockHash,
		"tx_hash":          event.TxHash,
		"tx_index":         event.TxIndex,
		"event_index":      event.EventIndex,
		"event_type":       event.EventType,
		"accounts":         event.Accounts,
		"program_id":       event.ProgramId,
		"commitment_level": commitmentToString(event.CommitmentLevel),
		"native_value":     event.NativeValue,
		"timestamp":        event.Timestamp.UTC().Format(time.RFC3339Nano),
		"payload":          event.Payload,
	}
}

// chainToString converts a Chain enum to its string representation.
func chainToString(c protov1.Chain) string {
	switch c {
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

// commitmentToString converts a CommitmentLevel enum to its string representation.
func commitmentToString(cl protov1.CommitmentLevel) string {
	switch cl {
	case protov1.CommitmentLevel_COMMITMENT_LEVEL_PROCESSED:
		return "processed"
	case protov1.CommitmentLevel_COMMITMENT_LEVEL_CONFIRMED:
		return "confirmed"
	case protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED:
		return "finalized"
	default:
		return "unspecified"
	}
}
