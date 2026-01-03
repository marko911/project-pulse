// Package routing provides event routing logic for matching canonical events
// against active subscriptions and dispatching to destinations.
package routing

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	protov1 "github.com/mirador/pulse/pkg/proto/v1"

	"github.com/mirador/pulse/internal/delivery/subscription"
)

// Destination represents a target for routed events (e.g., WebSocket connection, gRPC stream).
type Destination interface {
	// ID returns the unique identifier for this destination.
	ID() string

	// Send delivers an event to the destination.
	// Returns an error if delivery fails.
	Send(ctx context.Context, event *protov1.CanonicalEvent) error

	// SendBatch delivers multiple events to the destination.
	// Default implementation calls Send for each event.
	SendBatch(ctx context.Context, events []*protov1.CanonicalEvent) error

	// Close releases resources associated with the destination.
	Close() error
}

// DestinationRegistry manages active destinations by client ID.
type DestinationRegistry interface {
	// Get retrieves a destination by client ID.
	Get(clientID string) (Destination, bool)

	// Register adds a destination for a client.
	Register(clientID string, dest Destination)

	// Unregister removes a destination.
	Unregister(clientID string)

	// All returns all registered destinations.
	All() map[string]Destination
}

// RouterConfig holds configuration for the event router.
type RouterConfig struct {
	// Workers is the number of concurrent workers for event processing.
	Workers int

	// BatchSize is the maximum number of events to process in a batch.
	BatchSize int

	// BatchTimeout is the maximum time to wait for a full batch.
	BatchTimeout time.Duration

	// Logger for routing operations.
	Logger *slog.Logger

	// FailedEventHandler is called when event delivery fails.
	// If nil, failures are logged and dropped.
	FailedEventHandler func(event *protov1.CanonicalEvent, clientID string, err error)
}

// DefaultRouterConfig returns sensible defaults for router configuration.
func DefaultRouterConfig() RouterConfig {
	return RouterConfig{
		Workers:      8,
		BatchSize:    100,
		BatchTimeout: 50 * time.Millisecond,
		Logger:       slog.Default(),
	}
}

// Router matches events against subscriptions and routes to destinations.
type Router struct {
	subscriptions subscription.Manager
	destinations  DestinationRegistry
	config        RouterConfig

	eventCh chan *protov1.CanonicalEvent
	wg      sync.WaitGroup
	done    chan struct{}

	// Metrics
	mu              sync.RWMutex
	eventsRouted    int64
	eventsDropped   int64
	matchesFound    int64
	routingErrors   int64
	// Timing metrics for profiling (in nanoseconds)
	totalMatchTimeNs    int64
	totalDeliveryTimeNs int64
	batchesProcessed    int64
}

// NewRouter creates a new event router.
func NewRouter(subs subscription.Manager, dests DestinationRegistry, cfg RouterConfig) *Router {
	if cfg.Workers <= 0 {
		cfg.Workers = 8
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.BatchTimeout <= 0 {
		cfg.BatchTimeout = 50 * time.Millisecond
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Router{
		subscriptions: subs,
		destinations:  dests,
		config:        cfg,
		eventCh:       make(chan *protov1.CanonicalEvent, cfg.Workers*cfg.BatchSize),
		done:          make(chan struct{}),
	}
}

// Start begins the routing workers.
func (r *Router) Start(ctx context.Context) {
	for i := 0; i < r.config.Workers; i++ {
		r.wg.Add(1)
		go r.worker(ctx, i)
	}
}

// Stop gracefully shuts down the router.
func (r *Router) Stop() {
	close(r.done)
	r.wg.Wait()
}

// Route queues an event for routing.
// Non-blocking; returns error if queue is full.
func (r *Router) Route(event *protov1.CanonicalEvent) error {
	select {
	case r.eventCh <- event:
		return nil
	default:
		r.mu.Lock()
		r.eventsDropped++
		r.mu.Unlock()
		return fmt.Errorf("event queue full, dropping event %s", event.EventId)
	}
}

// RouteSync routes an event synchronously.
// Blocks until routing is complete.
func (r *Router) RouteSync(ctx context.Context, event *protov1.CanonicalEvent) error {
	return r.routeEvent(ctx, event)
}

// RouteBatch routes multiple events synchronously.
func (r *Router) RouteBatch(ctx context.Context, events []*protov1.CanonicalEvent) error {
	return r.routeBatch(ctx, events)
}

func (r *Router) worker(ctx context.Context, id int) {
	defer r.wg.Done()

	batch := make([]*protov1.CanonicalEvent, 0, r.config.BatchSize)
	timer := time.NewTimer(r.config.BatchTimeout)
	defer timer.Stop()

	for {
		select {
		case <-r.done:
			// Drain remaining events
			if len(batch) > 0 {
				r.routeBatch(ctx, batch)
			}
			return

		case <-ctx.Done():
			return

		case event := <-r.eventCh:
			batch = append(batch, event)
			if len(batch) >= r.config.BatchSize {
				r.routeBatch(ctx, batch)
				batch = batch[:0]
				timer.Reset(r.config.BatchTimeout)
			}

		case <-timer.C:
			if len(batch) > 0 {
				r.routeBatch(ctx, batch)
				batch = batch[:0]
			}
			timer.Reset(r.config.BatchTimeout)
		}
	}
}

func (r *Router) routeBatch(ctx context.Context, events []*protov1.CanonicalEvent) error {
	if len(events) == 0 {
		return nil
	}

	// Track match time for profiling
	matchStart := time.Now()

	// Get all matches in a single call
	matches, err := r.subscriptions.MatchBatch(ctx, events)
	if err != nil {
		r.config.Logger.Error("batch match failed", "error", err)
		return err
	}

	matchDuration := time.Since(matchStart)

	// Group events by destination (client)
	destEvents := make(map[string][]*protov1.CanonicalEvent)

	for _, event := range events {
		eventMatches := matches[event.EventId]
		for _, m := range eventMatches {
			destEvents[m.ClientID] = append(destEvents[m.ClientID], event)

			r.mu.Lock()
			r.matchesFound++
			r.mu.Unlock()
		}
	}

	// Track delivery time for profiling
	deliveryStart := time.Now()

	// Deliver to each destination concurrently
	var wg sync.WaitGroup
	for clientID, clientEvents := range destEvents {
		wg.Add(1)
		go func(cid string, evts []*protov1.CanonicalEvent) {
			defer wg.Done()
			r.deliverToClient(ctx, cid, evts)
		}(clientID, clientEvents)
	}
	wg.Wait()

	deliveryDuration := time.Since(deliveryStart)

	// Update timing metrics
	r.mu.Lock()
	r.eventsRouted += int64(len(events))
	r.totalMatchTimeNs += matchDuration.Nanoseconds()
	r.totalDeliveryTimeNs += deliveryDuration.Nanoseconds()
	r.batchesProcessed++
	r.mu.Unlock()

	return nil
}

func (r *Router) routeEvent(ctx context.Context, event *protov1.CanonicalEvent) error {
	matches, err := r.subscriptions.Match(ctx, event)
	if err != nil {
		r.config.Logger.Error("match failed", "event_id", event.EventId, "error", err)
		return err
	}

	if len(matches) == 0 {
		return nil
	}

	r.mu.Lock()
	r.matchesFound += int64(len(matches))
	r.eventsRouted++
	r.mu.Unlock()

	// Deliver to each matching destination
	var wg sync.WaitGroup
	for _, m := range matches {
		wg.Add(1)
		go func(clientID string) {
			defer wg.Done()
			r.deliverToClient(ctx, clientID, []*protov1.CanonicalEvent{event})
		}(m.ClientID)
	}
	wg.Wait()

	return nil
}

func (r *Router) deliverToClient(ctx context.Context, clientID string, events []*protov1.CanonicalEvent) {
	dest, ok := r.destinations.Get(clientID)
	if !ok {
		r.config.Logger.Debug("destination not found", "client_id", clientID)
		return
	}

	var err error
	if len(events) == 1 {
		err = dest.Send(ctx, events[0])
	} else {
		err = dest.SendBatch(ctx, events)
	}

	if err != nil {
		r.mu.Lock()
		r.routingErrors++
		r.mu.Unlock()

		if r.config.FailedEventHandler != nil {
			for _, e := range events {
				r.config.FailedEventHandler(e, clientID, err)
			}
		} else {
			r.config.Logger.Warn("delivery failed",
				"client_id", clientID,
				"event_count", len(events),
				"error", err,
			)
		}
	}
}

// Stats returns current router statistics.
func (r *Router) Stats() RouterStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := RouterStats{
		EventsRouted:     r.eventsRouted,
		EventsDropped:    r.eventsDropped,
		MatchesFound:     r.matchesFound,
		RoutingErrors:    r.routingErrors,
		QueueLength:      len(r.eventCh),
		QueueCapacity:    cap(r.eventCh),
		BatchesProcessed: r.batchesProcessed,
	}

	// Calculate average times per batch
	if r.batchesProcessed > 0 {
		stats.AvgMatchTimeUs = float64(r.totalMatchTimeNs) / float64(r.batchesProcessed) / 1000
		stats.AvgDeliveryTimeUs = float64(r.totalDeliveryTimeNs) / float64(r.batchesProcessed) / 1000
	}

	return stats
}

// RouterStats contains router performance metrics.
type RouterStats struct {
	EventsRouted     int64
	EventsDropped    int64
	MatchesFound     int64
	RoutingErrors    int64
	QueueLength      int
	QueueCapacity    int
	BatchesProcessed int64
	// Average time per batch in microseconds
	AvgMatchTimeUs    float64
	AvgDeliveryTimeUs float64
}

// InMemoryDestinationRegistry is a simple in-memory destination registry.
type InMemoryDestinationRegistry struct {
	mu    sync.RWMutex
	dests map[string]Destination
}

// NewInMemoryDestinationRegistry creates a new in-memory registry.
func NewInMemoryDestinationRegistry() *InMemoryDestinationRegistry {
	return &InMemoryDestinationRegistry{
		dests: make(map[string]Destination),
	}
}

func (r *InMemoryDestinationRegistry) Get(clientID string) (Destination, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.dests[clientID]
	return d, ok
}

func (r *InMemoryDestinationRegistry) Register(clientID string, dest Destination) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dests[clientID] = dest
}

func (r *InMemoryDestinationRegistry) Unregister(clientID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.dests, clientID)
}

func (r *InMemoryDestinationRegistry) All() map[string]Destination {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string]Destination, len(r.dests))
	for k, v := range r.dests {
		result[k] = v
	}
	return result
}
