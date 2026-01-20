// Package solana_ws implements the Solana blockchain adapter using standard WebSockets.
package solana_ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/marko911/project-pulse/internal/adapter"
)

// Adapter implements the Solana blockchain adapter using WebSockets.
type Adapter struct {
	cfg    Config
	logger *slog.Logger

	mu      sync.RWMutex
	running bool
	cancel  context.CancelFunc
	conn    *websocket.Conn
}

// Config holds Solana WebSocket adapter configuration.
type Config struct {
	adapter.Config

	// WebSocket endpoint (e.g. wss://api.mainnet-beta.solana.com)
	Endpoint string
}

// New creates a new Solana WebSocket adapter.
func New(cfg Config, logger *slog.Logger) *Adapter {
	if logger == nil {
		logger = slog.Default()
	}
	return &Adapter{
		cfg:    cfg,
		logger: logger.With("adapter", "solana-ws"),
	}
}

// Start begins streaming events from Solana via WebSockets.
func (a *Adapter) Start(ctx context.Context, events chan<- adapter.Event) error {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return fmt.Errorf("adapter already running")
	}

	ctx, cancel := context.WithCancel(ctx)
	a.cancel = cancel
	a.running = true
	a.mu.Unlock()

	a.logger.Info("starting solana websocket adapter",
		"endpoint", a.cfg.Endpoint,
		"commitment", a.cfg.CommitmentLevel,
	)

	go a.connectionLoop(ctx, events)

	return nil
}

// connectionLoop manages the WebSocket connection and reconnection.
func (a *Adapter) connectionLoop(ctx context.Context, events chan<- adapter.Event) {
	defer func() {
		a.mu.Lock()
		a.running = false
		if a.conn != nil {
			a.conn.Close()
			a.conn = nil
		}
		a.mu.Unlock()
		a.logger.Info("solana websocket adapter stopped")
	}()

	backoff := time.Second
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := a.connectAndStream(ctx, events); err != nil {
			if ctx.Err() != nil {
				return
			}

			a.logger.Error("websocket error, reconnecting",
				"error", err,
				"backoff", backoff,
			)

			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}

			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		} else {
			backoff = time.Second
		}
	}
}

// connectAndStream connects to the WebSocket and streams messages.
func (a *Adapter) connectAndStream(ctx context.Context, events chan<- adapter.Event) error {
	// Handle https->wss conversion for usability
	endpoint := a.cfg.Endpoint
	if len(endpoint) > 5 && endpoint[:5] == "https" {
		endpoint = "wss" + endpoint[5:]
	} else if len(endpoint) > 4 && endpoint[:4] == "http" {
		endpoint = "ws" + endpoint[4:]
	}

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}

	a.mu.Lock()
	a.conn = conn
	a.mu.Unlock()

	// Subscribe to slots (blocks)
	if err := a.subscribeSlot(conn); err != nil {
		conn.Close()
		return err
	}

	// Subscribe to logs (transactions)
	if err := a.subscribeLogs(conn); err != nil {
		conn.Close()
		return err
	}

	// Read loop
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read error: %w", err)
		}

		a.handleMessage(message, events)
	}
}

// handleMessage parses the incoming JSON message.
func (a *Adapter) handleMessage(msg []byte, events chan<- adapter.Event) {
	var base struct {
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}

	if err := json.Unmarshal(msg, &base); err != nil {
		return // Ignore malformed messages
	}

	if base.Method == "slotNotification" {
		a.handleSlotNotification(base.Params, events)
	} else if base.Method == "logsNotification" {
		a.handleLogsNotification(base.Params, events)
	}
}

func (a *Adapter) handleSlotNotification(params json.RawMessage, events chan<- adapter.Event) {
	var p struct {
		Result struct {
			Slot       uint64 `json:"slot"`
			Parent     uint64 `json:"parent"`
			Root       uint64 `json:"root"`
		} `json:"result"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return
	}

	// We don't emit raw slot events to the broker in this MVP, 
	// but we could use them for watermark tracking.
	a.logger.Debug("slot update", "slot", p.Result.Slot)
}

func (a *Adapter) handleLogsNotification(params json.RawMessage, events chan<- adapter.Event) {
	var p struct {
		Result struct {
			Context struct {
				Slot uint64 `json:"slot"`
			} `json:"context"`
			Value struct {
				Signature string   `json:"signature"`
				Err       any      `json:"err"`
				Logs      []string `json:"logs"`
			} `json:"value"`
		} `json:"result"`
	}

	if err := json.Unmarshal(params, &p); err != nil {
		return
	}

	// Emit event
	events <- adapter.Event{
		Chain:           "solana",
		CommitmentLevel: a.cfg.CommitmentLevel,
		BlockNumber:     p.Result.Context.Slot,
		TxHash:          p.Result.Value.Signature,
		EventType:       "transaction", // Simplified type
		Timestamp:       time.Now().Unix(),
		Payload:         params, // Store full JSON payload
	}
}

func (a *Adapter) subscribeSlot(conn *websocket.Conn) error {
	req := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "slotSubscribe",
	}
	return conn.WriteJSON(req)
}

func (a *Adapter) subscribeLogs(conn *websocket.Conn) error {
	req := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      2,
		"method":  "logsSubscribe",
		"params": []interface{}{
			"all", // Subscribe to all transactions (Alchemy Devnet supports this)
			map[string]interface{}{"commitment": a.cfg.CommitmentLevel},
		},
	}
	return conn.WriteJSON(req)
}

// Stop stops the adapter.
func (a *Adapter) Stop(ctx context.Context) error {
	a.mu.Lock()
	cancel := a.cancel
	a.running = false
	a.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	return nil
}

// Health checks connection status.
func (a *Adapter) Health(ctx context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if !a.running || a.conn == nil {
		return fmt.Errorf("not connected")
	}
	return nil
}
