package websocket

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func setupTestServer(t *testing.T) (*httptest.Server, *websocket.Conn, *Destination) {
	var serverDest *Destination
	serverReady := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade error: %v", err)
			return
		}

		serverDest = NewDestination(DestinationConfig{
			ID:             "test-client-1",
			Conn:           conn,
			SendBufferSize: 10,
		})

		close(serverReady)
	}))

	// Connect client
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	clientConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}

	<-serverReady

	return server, clientConn, serverDest
}

func TestDestination_ID(t *testing.T) {
	server, clientConn, dest := setupTestServer(t)
	defer server.Close()
	defer clientConn.Close()
	defer dest.Close()

	if dest.ID() != "test-client-1" {
		t.Errorf("expected ID 'test-client-1', got '%s'", dest.ID())
	}
}

func TestDestination_Send(t *testing.T) {
	server, clientConn, dest := setupTestServer(t)
	defer server.Close()
	defer clientConn.Close()

	// Start write pump in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dest.writePump(ctx)

	// Send an event
	event := &protov1.CanonicalEvent{
		EventId:     "evt-123",
		Chain:       protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber: 12345,
		BlockHash:   "0xabc",
		TxHash:      "0xdef",
		EventType:   "transfer",
		Accounts:    []string{"0x111", "0x222"},
		Timestamp:   time.Now(),
	}

	err := dest.Send(ctx, event)
	if err != nil {
		t.Fatalf("send error: %v", err)
	}

	// Read from client
	clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, message, err := clientConn.ReadMessage()
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	var msg ServerMessage
	if err := json.Unmarshal(message, &msg); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if msg.Type != "event" {
		t.Errorf("expected type 'event', got '%s'", msg.Type)
	}

	data := msg.Data.(map[string]interface{})
	if data["event_id"] != "evt-123" {
		t.Errorf("expected event_id 'evt-123', got '%v'", data["event_id"])
	}
}

func TestDestination_SendBatch(t *testing.T) {
	server, clientConn, dest := setupTestServer(t)
	defer server.Close()
	defer clientConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dest.writePump(ctx)

	events := []*protov1.CanonicalEvent{
		{
			EventId:   "evt-1",
			Chain:     protov1.Chain_CHAIN_ETHEREUM,
			EventType: "transfer",
			Timestamp: time.Now(),
		},
		{
			EventId:   "evt-2",
			Chain:     protov1.Chain_CHAIN_SOLANA,
			EventType: "swap",
			Timestamp: time.Now(),
		},
	}

	err := dest.SendBatch(ctx, events)
	if err != nil {
		t.Fatalf("send batch error: %v", err)
	}

	clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, message, err := clientConn.ReadMessage()
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	var msg ServerMessage
	if err := json.Unmarshal(message, &msg); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if msg.Type != "events" {
		t.Errorf("expected type 'events', got '%s'", msg.Type)
	}

	data := msg.Data.(map[string]interface{})
	count := int(data["count"].(float64))
	if count != 2 {
		t.Errorf("expected count 2, got %d", count)
	}
}

func TestDestination_Close(t *testing.T) {
	server, clientConn, dest := setupTestServer(t)
	defer server.Close()
	defer clientConn.Close()

	// Close destination
	err := dest.Close()
	if err != nil {
		t.Fatalf("close error: %v", err)
	}

	// Should be marked as closed
	if !dest.IsClosed() {
		t.Error("expected destination to be closed")
	}

	// Send should fail after close
	event := &protov1.CanonicalEvent{EventId: "evt-1"}
	err = dest.Send(context.Background(), event)
	if err == nil {
		t.Error("expected error when sending to closed destination")
	}
}

func TestDestination_OnCloseCallback(t *testing.T) {
	var closedID string
	callbackCalled := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _ := upgrader.Upgrade(w, r, nil)

		dest := NewDestination(DestinationConfig{
			ID:   "callback-test",
			Conn: conn,
			OnClose: func(id string) {
				closedID = id
				close(callbackCalled)
			},
		})

		dest.Close()
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	clientConn, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)
	defer clientConn.Close()

	select {
	case <-callbackCalled:
		if closedID != "callback-test" {
			t.Errorf("expected closed ID 'callback-test', got '%s'", closedID)
		}
	case <-time.After(2 * time.Second):
		t.Error("OnClose callback not called")
	}
}

func TestEventToJSON(t *testing.T) {
	ts := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	event := &protov1.CanonicalEvent{
		EventId:         "evt-json-test",
		Chain:           protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:     99999,
		BlockHash:       "0xblockhash",
		TxHash:          "0xtxhash",
		EventIndex:      5,
		EventType:       "swap",
		Accounts:        []string{"0xacc1", "0xacc2"},
		ProgramId:       "0xprogram",
		CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
		NativeValue:     1000000,
		Timestamp:       ts,
		Payload:         []byte(`{"key":"value"}`),
	}

	result := eventToJSON(event)

	if result["event_id"] != "evt-json-test" {
		t.Errorf("unexpected event_id: %v", result["event_id"])
	}
	if result["block_number"].(uint64) != 99999 {
		t.Errorf("unexpected block_number: %v", result["block_number"])
	}
	if result["event_type"] != "swap" {
		t.Errorf("unexpected event_type: %v", result["event_type"])
	}

	accounts := result["accounts"].([]string)
	if len(accounts) != 2 {
		t.Errorf("expected 2 accounts, got %d", len(accounts))
	}
}
