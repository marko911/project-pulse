//go:build integration

package subscription

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
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

	got, _ := mgr.Get(ctx, subID)
	if got == nil {
		t.Fatal("Subscription should exist before unsubscribe")
	}

	if err := mgr.Unsubscribe(ctx, subID); err != nil {
		t.Fatalf("Unsubscribe failed: %v", err)
	}

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

	for i := 0; i < 3; i++ {
		sub := &Subscription{
			ClientID: "client_multi",
			Filter: Filter{
				EventTypes: []string{"event_" + string(rune('a'+i))},
			},
		}
		mgr.Subscribe(ctx, sub)
	}

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

	for i := 0; i < 3; i++ {
		mgr.Subscribe(ctx, &Subscription{
			ClientID: "client_unsub_all",
			Filter:   Filter{},
		})
	}

	subs, _ := mgr.ListByClient(ctx, "client_unsub_all")
	if len(subs) != 3 {
		t.Fatalf("Expected 3 subs before unsubscribe, got %d", len(subs))
	}

	if err := mgr.UnsubscribeAll(ctx, "client_unsub_all"); err != nil {
		t.Fatalf("UnsubscribeAll failed: %v", err)
	}

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

	mgr.Subscribe(ctx, &Subscription{
		ClientID: "wildcard_client",
		Filter:   Filter{},
	})

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

	pastTime := time.Now().Add(-time.Hour)
	mgr.Subscribe(ctx, &Subscription{
		ClientID:  "expired_client",
		Filter:    Filter{},
		ExpiresAt: pastTime,
	})

	futureTime := time.Now().Add(time.Hour)
	mgr.Subscribe(ctx, &Subscription{
		ClientID:  "valid_client",
		Filter:    Filter{},
		ExpiresAt: futureTime,
	})

	cleaned, err := mgr.Cleanup(ctx)
	if err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}

	if cleaned != 1 {
		t.Errorf("Expected 1 cleaned, got %d", cleaned)
	}

	subs, _ := mgr.ListByClient(ctx, "valid_client")
	if len(subs) != 1 {
		t.Error("Valid subscription should still exist")
	}

	subs, _ = mgr.ListByClient(ctx, "expired_client")
	if len(subs) != 0 {
		t.Error("Expired subscription should be removed")
	}
}

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
