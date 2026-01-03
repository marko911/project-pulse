package routing

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	protov1 "github.com/mirador/pulse/pkg/proto/v1"

	"github.com/mirador/pulse/internal/delivery/subscription"
)

// mockSubscriptionManager implements subscription.Manager for testing.
type mockSubscriptionManager struct {
	subscriptions map[string]*subscription.Subscription
	mu            sync.RWMutex
}

func newMockSubscriptionManager() *mockSubscriptionManager {
	return &mockSubscriptionManager{
		subscriptions: make(map[string]*subscription.Subscription),
	}
}

func (m *mockSubscriptionManager) Subscribe(_ context.Context, sub *subscription.Subscription) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if sub.ID == "" {
		sub.ID = "sub_" + sub.ClientID
	}
	m.subscriptions[sub.ID] = sub
	return sub.ID, nil
}

func (m *mockSubscriptionManager) Unsubscribe(_ context.Context, subID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.subscriptions, subID)
	return nil
}

func (m *mockSubscriptionManager) UnsubscribeAll(_ context.Context, clientID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, sub := range m.subscriptions {
		if sub.ClientID == clientID {
			delete(m.subscriptions, id)
		}
	}
	return nil
}

func (m *mockSubscriptionManager) Get(_ context.Context, subID string) (*subscription.Subscription, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.subscriptions[subID], nil
}

func (m *mockSubscriptionManager) ListByClient(_ context.Context, clientID string) ([]*subscription.Subscription, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*subscription.Subscription
	for _, sub := range m.subscriptions {
		if sub.ClientID == clientID {
			result = append(result, sub)
		}
	}
	return result, nil
}

func (m *mockSubscriptionManager) Match(_ context.Context, event *protov1.CanonicalEvent) ([]subscription.MatchResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var results []subscription.MatchResult
	for _, sub := range m.subscriptions {
		if sub.Filter.Matches(event) {
			results = append(results, subscription.MatchResult{
				SubscriptionID: sub.ID,
				ClientID:       sub.ClientID,
			})
		}
	}
	return results, nil
}

func (m *mockSubscriptionManager) MatchBatch(ctx context.Context, events []*protov1.CanonicalEvent) (map[string][]subscription.MatchResult, error) {
	results := make(map[string][]subscription.MatchResult)
	for _, event := range events {
		matches, err := m.Match(ctx, event)
		if err != nil {
			return nil, err
		}
		results[event.EventId] = matches
	}
	return results, nil
}

func (m *mockSubscriptionManager) Refresh(_ context.Context, _ string, _ time.Time) error {
	return nil
}

func (m *mockSubscriptionManager) Cleanup(_ context.Context) (int, error) {
	return 0, nil
}

func (m *mockSubscriptionManager) Count(_ context.Context) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return int64(len(m.subscriptions)), nil
}

func (m *mockSubscriptionManager) Close() error {
	return nil
}

// mockDestination implements Destination for testing.
type mockDestination struct {
	id          string
	received    []*protov1.CanonicalEvent
	mu          sync.Mutex
	sendDelay   time.Duration
	failOnSend  bool
}

func newMockDestination(id string) *mockDestination {
	return &mockDestination{
		id:       id,
		received: make([]*protov1.CanonicalEvent, 0),
	}
}

func (d *mockDestination) ID() string {
	return d.id
}

func (d *mockDestination) Send(_ context.Context, event *protov1.CanonicalEvent) error {
	if d.sendDelay > 0 {
		time.Sleep(d.sendDelay)
	}
	if d.failOnSend {
		return context.DeadlineExceeded
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.received = append(d.received, event)
	return nil
}

func (d *mockDestination) SendBatch(ctx context.Context, events []*protov1.CanonicalEvent) error {
	for _, e := range events {
		if err := d.Send(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

func (d *mockDestination) Close() error {
	return nil
}

func (d *mockDestination) ReceivedCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.received)
}

func (d *mockDestination) ReceivedEvents() []*protov1.CanonicalEvent {
	d.mu.Lock()
	defer d.mu.Unlock()
	result := make([]*protov1.CanonicalEvent, len(d.received))
	copy(result, d.received)
	return result
}

func TestRouter_RouteSync(t *testing.T) {
	ctx := context.Background()

	// Setup
	subMgr := newMockSubscriptionManager()
	destReg := NewInMemoryDestinationRegistry()

	// Create subscription
	subMgr.Subscribe(ctx, &subscription.Subscription{
		ClientID: "client_1",
		Filter: subscription.Filter{
			Chains: []protov1.Chain{protov1.Chain_CHAIN_ETHEREUM},
		},
	})

	// Create destination
	dest := newMockDestination("client_1")
	destReg.Register("client_1", dest)

	// Create router
	cfg := DefaultRouterConfig()
	router := NewRouter(subMgr, destReg, cfg)

	// Route event
	event := &protov1.CanonicalEvent{
		EventId: "evt_1",
		Chain:   protov1.Chain_CHAIN_ETHEREUM,
	}

	err := router.RouteSync(ctx, event)
	if err != nil {
		t.Fatalf("RouteSync failed: %v", err)
	}

	// Give time for async delivery
	time.Sleep(10 * time.Millisecond)

	// Verify
	if dest.ReceivedCount() != 1 {
		t.Errorf("Expected 1 event, got %d", dest.ReceivedCount())
	}

	stats := router.Stats()
	if stats.EventsRouted != 1 {
		t.Errorf("Expected 1 routed event, got %d", stats.EventsRouted)
	}
	if stats.MatchesFound != 1 {
		t.Errorf("Expected 1 match, got %d", stats.MatchesFound)
	}
}

func TestRouter_NoMatch(t *testing.T) {
	ctx := context.Background()

	subMgr := newMockSubscriptionManager()
	destReg := NewInMemoryDestinationRegistry()

	// Subscription for Ethereum
	subMgr.Subscribe(ctx, &subscription.Subscription{
		ClientID: "client_1",
		Filter: subscription.Filter{
			Chains: []protov1.Chain{protov1.Chain_CHAIN_ETHEREUM},
		},
	})

	dest := newMockDestination("client_1")
	destReg.Register("client_1", dest)

	router := NewRouter(subMgr, destReg, DefaultRouterConfig())

	// Route Solana event (no match)
	event := &protov1.CanonicalEvent{
		EventId: "evt_1",
		Chain:   protov1.Chain_CHAIN_SOLANA,
	}

	router.RouteSync(ctx, event)
	time.Sleep(10 * time.Millisecond)

	if dest.ReceivedCount() != 0 {
		t.Errorf("Expected 0 events for non-matching chain, got %d", dest.ReceivedCount())
	}
}

func TestRouter_MultipleSubscriptions(t *testing.T) {
	ctx := context.Background()

	subMgr := newMockSubscriptionManager()
	destReg := NewInMemoryDestinationRegistry()

	// Two subscriptions for different clients, both matching Ethereum
	subMgr.Subscribe(ctx, &subscription.Subscription{
		ID:       "sub_1",
		ClientID: "client_1",
		Filter: subscription.Filter{
			Chains: []protov1.Chain{protov1.Chain_CHAIN_ETHEREUM},
		},
	})
	subMgr.Subscribe(ctx, &subscription.Subscription{
		ID:       "sub_2",
		ClientID: "client_2",
		Filter: subscription.Filter{
			EventTypes: []string{"transfer"},
		},
	})

	dest1 := newMockDestination("client_1")
	dest2 := newMockDestination("client_2")
	destReg.Register("client_1", dest1)
	destReg.Register("client_2", dest2)

	router := NewRouter(subMgr, destReg, DefaultRouterConfig())

	// Event matches both subscriptions
	event := &protov1.CanonicalEvent{
		EventId:   "evt_1",
		Chain:     protov1.Chain_CHAIN_ETHEREUM,
		EventType: "transfer",
	}

	router.RouteSync(ctx, event)
	time.Sleep(50 * time.Millisecond)

	if dest1.ReceivedCount() != 1 {
		t.Errorf("client_1: expected 1 event, got %d", dest1.ReceivedCount())
	}
	if dest2.ReceivedCount() != 1 {
		t.Errorf("client_2: expected 1 event, got %d", dest2.ReceivedCount())
	}
}

func TestRouter_BatchRouting(t *testing.T) {
	ctx := context.Background()

	subMgr := newMockSubscriptionManager()
	destReg := NewInMemoryDestinationRegistry()

	subMgr.Subscribe(ctx, &subscription.Subscription{
		ClientID: "client_1",
		Filter:   subscription.Filter{}, // Match all
	})

	dest := newMockDestination("client_1")
	destReg.Register("client_1", dest)

	router := NewRouter(subMgr, destReg, DefaultRouterConfig())

	// Route batch
	events := make([]*protov1.CanonicalEvent, 10)
	for i := 0; i < 10; i++ {
		events[i] = &protov1.CanonicalEvent{
			EventId: "evt_" + string(rune('0'+i)),
			Chain:   protov1.Chain_CHAIN_ETHEREUM,
		}
	}

	err := router.RouteBatch(ctx, events)
	if err != nil {
		t.Fatalf("RouteBatch failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if dest.ReceivedCount() != 10 {
		t.Errorf("Expected 10 events, got %d", dest.ReceivedCount())
	}
}

func TestRouter_AsyncWorkers(t *testing.T) {
	ctx := context.Background()

	subMgr := newMockSubscriptionManager()
	destReg := NewInMemoryDestinationRegistry()

	subMgr.Subscribe(ctx, &subscription.Subscription{
		ClientID: "client_1",
		Filter:   subscription.Filter{},
	})

	dest := newMockDestination("client_1")
	destReg.Register("client_1", dest)

	cfg := DefaultRouterConfig()
	cfg.Workers = 4
	cfg.BatchSize = 5
	cfg.BatchTimeout = 20 * time.Millisecond

	router := NewRouter(subMgr, destReg, cfg)
	router.Start(ctx)
	defer router.Stop()

	// Queue multiple events
	for i := 0; i < 20; i++ {
		event := &protov1.CanonicalEvent{
			EventId: "evt_" + string(rune('a'+i)),
			Chain:   protov1.Chain_CHAIN_ETHEREUM,
		}
		router.Route(event)
	}

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	received := dest.ReceivedCount()
	if received != 20 {
		t.Errorf("Expected 20 events, got %d", received)
	}

	stats := router.Stats()
	if stats.EventsRouted != 20 {
		t.Errorf("Expected 20 routed, got %d", stats.EventsRouted)
	}
}

func TestRouter_DestinationNotFound(t *testing.T) {
	ctx := context.Background()

	subMgr := newMockSubscriptionManager()
	destReg := NewInMemoryDestinationRegistry()

	// Subscription exists but destination doesn't
	subMgr.Subscribe(ctx, &subscription.Subscription{
		ClientID: "missing_client",
		Filter:   subscription.Filter{},
	})

	router := NewRouter(subMgr, destReg, DefaultRouterConfig())

	event := &protov1.CanonicalEvent{
		EventId: "evt_1",
		Chain:   protov1.Chain_CHAIN_ETHEREUM,
	}

	// Should not error, just skip the missing destination
	err := router.RouteSync(ctx, event)
	if err != nil {
		t.Errorf("RouteSync should not error for missing destination: %v", err)
	}
}

func TestRouter_FailedDelivery(t *testing.T) {
	ctx := context.Background()

	subMgr := newMockSubscriptionManager()
	destReg := NewInMemoryDestinationRegistry()

	subMgr.Subscribe(ctx, &subscription.Subscription{
		ClientID: "client_1",
		Filter:   subscription.Filter{},
	})

	dest := newMockDestination("client_1")
	dest.failOnSend = true
	destReg.Register("client_1", dest)

	var failedCount int32
	cfg := DefaultRouterConfig()
	cfg.FailedEventHandler = func(event *protov1.CanonicalEvent, clientID string, err error) {
		atomic.AddInt32(&failedCount, 1)
	}

	router := NewRouter(subMgr, destReg, cfg)

	event := &protov1.CanonicalEvent{
		EventId: "evt_1",
		Chain:   protov1.Chain_CHAIN_ETHEREUM,
	}

	router.RouteSync(ctx, event)
	time.Sleep(10 * time.Millisecond)

	if atomic.LoadInt32(&failedCount) != 1 {
		t.Errorf("Expected 1 failed event, got %d", failedCount)
	}

	stats := router.Stats()
	if stats.RoutingErrors != 1 {
		t.Errorf("Expected 1 routing error, got %d", stats.RoutingErrors)
	}
}

func TestInMemoryDestinationRegistry(t *testing.T) {
	reg := NewInMemoryDestinationRegistry()

	dest1 := newMockDestination("dest_1")
	dest2 := newMockDestination("dest_2")

	// Register
	reg.Register("client_1", dest1)
	reg.Register("client_2", dest2)

	// Get
	got, ok := reg.Get("client_1")
	if !ok || got.ID() != "dest_1" {
		t.Error("Failed to get client_1")
	}

	// All
	all := reg.All()
	if len(all) != 2 {
		t.Errorf("Expected 2 destinations, got %d", len(all))
	}

	// Unregister
	reg.Unregister("client_1")
	_, ok = reg.Get("client_1")
	if ok {
		t.Error("client_1 should be unregistered")
	}
}
