//go:build integration

package subscription

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	protov1 "github.com/mirador/pulse/pkg/proto/v1"
)

func getTestRedisClient(t *testing.T) *redis.Client {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	return client
}

func TestRedisManager_Subscribe(t *testing.T) {
	client := getTestRedisClient(t)
	defer client.Close()

	// Use unique prefix for test isolation
	prefix := "test:" + time.Now().Format("20060102150405") + ":"
	mgr := NewRedisManagerWithClient(client, prefix)
	defer cleanup(t, client, prefix)

	ctx := context.Background()

	sub := &Subscription{
		ClientID: "client_1",
		Filter: Filter{
			Chains:     []protov1.Chain{protov1.Chain_CHAIN_ETHEREUM},
			EventTypes: []string{"transfer"},
		},
	}

	subID, err := mgr.Subscribe(ctx, sub)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	if subID == "" {
		t.Fatal("Expected non-empty subscription ID")
	}

	// Verify subscription exists
	got, err := mgr.Get(ctx, subID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got == nil {
		t.Fatal("Expected subscription to exist")
	}
	if got.ClientID != "client_1" {
		t.Errorf("ClientID = %s, want client_1", got.ClientID)
	}
}

func TestRedisManager_Match(t *testing.T) {
	client := getTestRedisClient(t)
	defer client.Close()

	prefix := "test:" + time.Now().Format("20060102150405") + ":"
	mgr := NewRedisManagerWithClient(client, prefix)
	defer cleanup(t, client, prefix)

	ctx := context.Background()

	// Create subscriptions with different filters
	sub1 := &Subscription{
		ClientID: "client_1",
		Filter: Filter{
			Chains: []protov1.Chain{protov1.Chain_CHAIN_ETHEREUM},
		},
	}
	sub2 := &Subscription{
		ClientID: "client_2",
		Filter: Filter{
			EventTypes: []string{"transfer"},
		},
	}
	sub3 := &Subscription{
		ClientID: "client_3",
		Filter: Filter{
			Chains:     []protov1.Chain{protov1.Chain_CHAIN_SOLANA},
			EventTypes: []string{"swap"},
		},
	}

	mgr.Subscribe(ctx, sub1)
	mgr.Subscribe(ctx, sub2)
	mgr.Subscribe(ctx, sub3)

	// Event that matches sub1 and sub2
	event := &protov1.CanonicalEvent{
		EventId:   "evt_1",
		Chain:     protov1.Chain_CHAIN_ETHEREUM,
		EventType: "transfer",
	}

	matches, err := mgr.Match(ctx, event)
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	if len(matches) != 2 {
		t.Errorf("Expected 2 matches, got %d", len(matches))
	}

	clientIDs := make(map[string]bool)
	for _, m := range matches {
		clientIDs[m.ClientID] = true
	}

	if !clientIDs["client_1"] {
		t.Error("Expected client_1 to match")
	}
	if !clientIDs["client_2"] {
		t.Error("Expected client_2 to match")
	}
	if clientIDs["client_3"] {
		t.Error("client_3 should not match")
	}
}

func TestRedisManager_Unsubscribe(t *testing.T) {
	client := getTestRedisClient(t)
	defer client.Close()

	prefix := "test:" + time.Now().Format("20060102150405") + ":"
	mgr := NewRedisManagerWithClient(client, prefix)
	defer cleanup(t, client, prefix)

	ctx := context.Background()

	sub := &Subscription{
		ClientID: "client_1",
		Filter:   Filter{},
	}

	subID, _ := mgr.Subscribe(ctx, sub)

	// Verify it exists
	got, _ := mgr.Get(ctx, subID)
	if got == nil {
		t.Fatal("Subscription should exist before unsubscribe")
	}

	// Unsubscribe
	if err := mgr.Unsubscribe(ctx, subID); err != nil {
		t.Fatalf("Unsubscribe failed: %v", err)
	}

	// Verify it's gone
	got, _ = mgr.Get(ctx, subID)
	if got != nil {
		t.Fatal("Subscription should not exist after unsubscribe")
	}
}

func TestRedisManager_ListByClient(t *testing.T) {
	client := getTestRedisClient(t)
	defer client.Close()

	prefix := "test:" + time.Now().Format("20060102150405") + ":"
	mgr := NewRedisManagerWithClient(client, prefix)
	defer cleanup(t, client, prefix)

	ctx := context.Background()

	// Create multiple subscriptions for same client
	for i := 0; i < 3; i++ {
		sub := &Subscription{
			ClientID: "client_multi",
			Filter: Filter{
				EventTypes: []string{"event_" + string(rune('a'+i))},
			},
		}
		mgr.Subscribe(ctx, sub)
	}

	// Also create one for different client
	mgr.Subscribe(ctx, &Subscription{
		ClientID: "other_client",
		Filter:   Filter{},
	})

	subs, err := mgr.ListByClient(ctx, "client_multi")
	if err != nil {
		t.Fatalf("ListByClient failed: %v", err)
	}

	if len(subs) != 3 {
		t.Errorf("Expected 3 subscriptions, got %d", len(subs))
	}
}

func TestRedisManager_UnsubscribeAll(t *testing.T) {
	client := getTestRedisClient(t)
	defer client.Close()

	prefix := "test:" + time.Now().Format("20060102150405") + ":"
	mgr := NewRedisManagerWithClient(client, prefix)
	defer cleanup(t, client, prefix)

	ctx := context.Background()

	// Create multiple subscriptions
	for i := 0; i < 3; i++ {
		mgr.Subscribe(ctx, &Subscription{
			ClientID: "client_unsub_all",
			Filter:   Filter{},
		})
	}

	// Verify they exist
	subs, _ := mgr.ListByClient(ctx, "client_unsub_all")
	if len(subs) != 3 {
		t.Fatalf("Expected 3 subs before unsubscribe, got %d", len(subs))
	}

	// Unsubscribe all
	if err := mgr.UnsubscribeAll(ctx, "client_unsub_all"); err != nil {
		t.Fatalf("UnsubscribeAll failed: %v", err)
	}

	// Verify they're gone
	subs, _ = mgr.ListByClient(ctx, "client_unsub_all")
	if len(subs) != 0 {
		t.Errorf("Expected 0 subs after unsubscribe, got %d", len(subs))
	}
}

func TestRedisManager_WildcardSubscription(t *testing.T) {
	client := getTestRedisClient(t)
	defer client.Close()

	prefix := "test:" + time.Now().Format("20060102150405") + ":"
	mgr := NewRedisManagerWithClient(client, prefix)
	defer cleanup(t, client, prefix)

	ctx := context.Background()

	// Create a wildcard subscription (empty filter)
	mgr.Subscribe(ctx, &Subscription{
		ClientID: "wildcard_client",
		Filter:   Filter{}, // matches everything
	})

	// Any event should match
	event := &protov1.CanonicalEvent{
		EventId:   "evt_wild",
		Chain:     protov1.Chain_CHAIN_POLYGON,
		EventType: "random_event",
	}

	matches, err := mgr.Match(ctx, event)
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	if len(matches) != 1 {
		t.Errorf("Expected 1 match for wildcard, got %d", len(matches))
	}
}

func TestRedisManager_Cleanup(t *testing.T) {
	client := getTestRedisClient(t)
	defer client.Close()

	prefix := "test:" + time.Now().Format("20060102150405") + ":"
	mgr := NewRedisManagerWithClient(client, prefix)
	defer cleanup(t, client, prefix)

	ctx := context.Background()

	// Create an already-expired subscription
	pastTime := time.Now().Add(-time.Hour)
	mgr.Subscribe(ctx, &Subscription{
		ClientID:  "expired_client",
		Filter:    Filter{},
		ExpiresAt: pastTime,
	})

	// Create a valid subscription
	futureTime := time.Now().Add(time.Hour)
	mgr.Subscribe(ctx, &Subscription{
		ClientID:  "valid_client",
		Filter:    Filter{},
		ExpiresAt: futureTime,
	})

	// Run cleanup
	cleaned, err := mgr.Cleanup(ctx)
	if err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}

	if cleaned != 1 {
		t.Errorf("Expected 1 cleaned, got %d", cleaned)
	}

	// Verify valid one still exists
	subs, _ := mgr.ListByClient(ctx, "valid_client")
	if len(subs) != 1 {
		t.Error("Valid subscription should still exist")
	}

	// Verify expired one is gone
	subs, _ = mgr.ListByClient(ctx, "expired_client")
	if len(subs) != 0 {
		t.Error("Expired subscription should be removed")
	}
}

// cleanup removes all test keys
func cleanup(t *testing.T, client *redis.Client, prefix string) {
	ctx := context.Background()
	var cursor uint64

	for {
		keys, nextCursor, err := client.Scan(ctx, cursor, prefix+"*", 1000).Result()
		if err != nil {
			t.Logf("Cleanup scan error: %v", err)
			return
		}

		if len(keys) > 0 {
			if err := client.Del(ctx, keys...).Err(); err != nil {
				t.Logf("Cleanup del error: %v", err)
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
}
