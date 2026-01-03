package websocket

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestManager_RegisterAndGet(t *testing.T) {
	manager := NewManager(ManagerConfig{})

	// Create a test connection
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		dest := manager.HandleConnection(context.Background(), "client-1", conn, nil)
		if dest == nil {
			t.Error("HandleConnection returned nil")
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	clientConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer clientConn.Close()

	// Give time for registration
	time.Sleep(100 * time.Millisecond)

	// Should be able to get the destination
	dest, ok := manager.Get("client-1")
	if !ok {
		t.Error("expected to find destination 'client-1'")
	}
	if dest.ID() != "client-1" {
		t.Errorf("expected ID 'client-1', got '%s'", dest.ID())
	}
}

func TestManager_Unregister(t *testing.T) {
	manager := NewManager(ManagerConfig{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _ := upgrader.Upgrade(w, r, nil)
		manager.HandleConnection(context.Background(), "client-unreg", conn, nil)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	clientConn, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)
	defer clientConn.Close()

	time.Sleep(100 * time.Millisecond)

	// Verify registered
	if _, ok := manager.Get("client-unreg"); !ok {
		t.Fatal("client should be registered")
	}

	// Unregister
	manager.Unregister("client-unreg")

	// Should not find it anymore
	if _, ok := manager.Get("client-unreg"); ok {
		t.Error("client should be unregistered")
	}
}

func TestManager_All(t *testing.T) {
	manager := NewManager(ManagerConfig{})

	connectionCount := 3
	ready := make(chan struct{}, connectionCount)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _ := upgrader.Upgrade(w, r, nil)
		clientID := r.URL.Query().Get("id")
		manager.HandleConnection(context.Background(), clientID, conn, nil)
		ready <- struct{}{}
	}))
	defer server.Close()

	// Create multiple connections
	var clients []*websocket.Conn
	for i := 0; i < connectionCount; i++ {
		wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "?id=client-" + string(rune('a'+i))
		conn, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)
		clients = append(clients, conn)
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()

	// Wait for all connections
	for i := 0; i < connectionCount; i++ {
		<-ready
	}

	// Get all destinations
	all := manager.All()
	if len(all) != connectionCount {
		t.Errorf("expected %d destinations, got %d", connectionCount, len(all))
	}
}

func TestManager_ActiveCount(t *testing.T) {
	manager := NewManager(ManagerConfig{})

	if manager.ActiveCount() != 0 {
		t.Error("expected 0 active connections initially")
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _ := upgrader.Upgrade(w, r, nil)
		manager.HandleConnection(context.Background(), "count-test", conn, nil)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	clientConn, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)

	time.Sleep(100 * time.Millisecond)

	if manager.ActiveCount() != 1 {
		t.Errorf("expected 1 active connection, got %d", manager.ActiveCount())
	}

	clientConn.Close()
	time.Sleep(100 * time.Millisecond)
}

func TestManager_Stats(t *testing.T) {
	manager := NewManager(ManagerConfig{})

	stats := manager.Stats()
	if stats.TotalConnections != 0 {
		t.Errorf("expected 0 total connections, got %d", stats.TotalConnections)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _ := upgrader.Upgrade(w, r, nil)
		manager.HandleConnection(context.Background(), "stats-test", conn, nil)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	clientConn, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)
	defer clientConn.Close()

	time.Sleep(100 * time.Millisecond)

	stats = manager.Stats()
	if stats.TotalConnections != 1 {
		t.Errorf("expected 1 total connection, got %d", stats.TotalConnections)
	}
	if stats.ActiveConnections != 1 {
		t.Errorf("expected 1 active connection, got %d", stats.ActiveConnections)
	}
}

func TestManager_ReplaceExistingConnection(t *testing.T) {
	manager := NewManager(ManagerConfig{})

	var firstDest *Destination
	connectionsDone := make(chan struct{}, 2)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _ := upgrader.Upgrade(w, r, nil)
		dest := manager.HandleConnection(context.Background(), "same-client", conn, nil)
		if firstDest == nil {
			firstDest = dest
		}
		connectionsDone <- struct{}{}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	// First connection
	conn1, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)
	defer conn1.Close()
	<-connectionsDone

	// Second connection with same ID should replace first
	conn2, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)
	defer conn2.Close()
	<-connectionsDone

	time.Sleep(100 * time.Millisecond)

	// First destination should be closed
	if !firstDest.IsClosed() {
		t.Error("first destination should be closed after replacement")
	}

	// Should still have exactly 1 active connection
	if manager.ActiveCount() != 1 {
		t.Errorf("expected 1 active connection after replacement, got %d", manager.ActiveCount())
	}
}

func TestManager_Close(t *testing.T) {
	manager := NewManager(ManagerConfig{})

	var dest *Destination
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _ := upgrader.Upgrade(w, r, nil)
		dest = manager.HandleConnection(context.Background(), "close-test", conn, nil)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	clientConn, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)
	defer clientConn.Close()

	time.Sleep(100 * time.Millisecond)

	// Close manager
	err := manager.Close()
	if err != nil {
		t.Fatalf("close error: %v", err)
	}

	// All connections should be closed
	if !dest.IsClosed() {
		t.Error("destination should be closed after manager.Close()")
	}

	if manager.ActiveCount() != 0 {
		t.Errorf("expected 0 active connections after close, got %d", manager.ActiveCount())
	}
}
