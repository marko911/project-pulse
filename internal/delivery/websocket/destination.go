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
	writeWait = 10 * time.Second

	pongWait = 60 * time.Second

	pingPeriod = (pongWait * 9) / 10

	maxMessageSize = 512 * 1024
)

type Destination struct {
	id       string
	conn     *websocket.Conn
	send     chan []byte
	done     chan struct{}
	mu       sync.RWMutex
	closed   bool
	metadata map[string]string

	onClose func(id string)
}

type DestinationConfig struct {
	ID string

	Conn *websocket.Conn

	SendBufferSize int

	Metadata map[string]string

	OnClose func(id string)
}

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

func (d *Destination) ID() string {
	return d.id
}

func (d *Destination) Metadata() map[string]string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.metadata
}

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
		return fmt.Errorf("send buffer full for client %s", d.id)
	}
}

func (d *Destination) SendBatch(ctx context.Context, events []*protov1.CanonicalEvent) error {
	d.mu.RLock()
	if d.closed {
		d.mu.RUnlock()
		return fmt.Errorf("destination closed")
	}
	d.mu.RUnlock()

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

func (d *Destination) IsClosed() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.closed
}

func (d *Destination) Run(ctx context.Context) {
	go d.writePump(ctx)
	d.readPump(ctx)
}

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
			}
			return
		}

		d.handleMessage(message)
	}
}

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
				d.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := d.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

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

func (d *Destination) handleMessage(message []byte) {
	var msg ClientMessage
	if err := json.Unmarshal(message, &msg); err != nil {
		return
	}

	switch msg.Type {
	case "ping":
		d.sendControlMessage("pong", nil)
	case "heartbeat":
	}
}

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

func (d *Destination) marshalEvent(event *protov1.CanonicalEvent) ([]byte, error) {
	msg := ServerMessage{
		Type:      "event",
		Timestamp: time.Now().UTC(),
		Data:      eventToJSON(event),
	}
	return json.Marshal(msg)
}

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

type ClientMessage struct {
	Type string          `json:"type"`
	ID   string          `json:"id,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

type ServerMessage struct {
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Data      interface{} `json:"data,omitempty"`
	Error     string      `json:"error,omitempty"`
}

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
