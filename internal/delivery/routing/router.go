package routing

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"

	"github.com/marko911/project-pulse/internal/delivery/subscription"
)

type Destination interface {
	ID() string

	Send(ctx context.Context, event *protov1.CanonicalEvent) error

	SendBatch(ctx context.Context, events []*protov1.CanonicalEvent) error

	Close() error
}

type DestinationRegistry interface {
	Get(clientID string) (Destination, bool)

	Register(clientID string, dest Destination)

	Unregister(clientID string)

	All() map[string]Destination
}

type RouterConfig struct {
	Workers int

	BatchSize int

	BatchTimeout time.Duration

	Logger *slog.Logger

	FailedEventHandler func(event *protov1.CanonicalEvent, clientID string, err error)
}

func DefaultRouterConfig() RouterConfig {
	return RouterConfig{
		Workers:      8,
		BatchSize:    100,
		BatchTimeout: 50 * time.Millisecond,
		Logger:       slog.Default(),
	}
}

type Router struct {
	subscriptions subscription.Manager
	destinations  DestinationRegistry
	config        RouterConfig

	eventCh chan *protov1.CanonicalEvent
	wg      sync.WaitGroup
	done    chan struct{}

	mu              sync.RWMutex
	eventsRouted    int64
	eventsDropped   int64
	matchesFound    int64
	routingErrors   int64
	totalMatchTimeNs    int64
	totalDeliveryTimeNs int64
	batchesProcessed    int64
}

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

func (r *Router) Start(ctx context.Context) {
	for i := 0; i < r.config.Workers; i++ {
		r.wg.Add(1)
		go r.worker(ctx, i)
	}
}

func (r *Router) Stop() {
	close(r.done)
	r.wg.Wait()
}

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

func (r *Router) RouteSync(ctx context.Context, event *protov1.CanonicalEvent) error {
	return r.routeEvent(ctx, event)
}

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

	matchStart := time.Now()

	matches, err := r.subscriptions.MatchBatch(ctx, events)
	if err != nil {
		r.config.Logger.Error("batch match failed", "error", err)
		return err
	}

	matchDuration := time.Since(matchStart)

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

	deliveryStart := time.Now()

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

	if r.batchesProcessed > 0 {
		stats.AvgMatchTimeUs = float64(r.totalMatchTimeNs) / float64(r.batchesProcessed) / 1000
		stats.AvgDeliveryTimeUs = float64(r.totalDeliveryTimeNs) / float64(r.batchesProcessed) / 1000
	}

	return stats
}

type RouterStats struct {
	EventsRouted     int64
	EventsDropped    int64
	MatchesFound     int64
	RoutingErrors    int64
	QueueLength      int
	QueueCapacity    int
	BatchesProcessed int64
	AvgMatchTimeUs    float64
	AvgDeliveryTimeUs float64
}

type InMemoryDestinationRegistry struct {
	mu    sync.RWMutex
	dests map[string]Destination
}

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
