package subscription

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	protov1 "github.com/mirador/pulse/pkg/proto/v1"
)

const (
	// Redis key prefixes
	keySubscription   = "sub:"           // sub:{id} -> Subscription JSON
	keyClientSubs     = "client:subs:"   // client:subs:{clientID} -> SET of sub IDs
	keyChainIndex     = "idx:chain:"     // idx:chain:{chain} -> SET of sub IDs
	keyAccountIndex   = "idx:account:"   // idx:account:{account} -> SET of sub IDs
	keyEventTypeIndex = "idx:eventtype:" // idx:eventtype:{type} -> SET of sub IDs
	keyProgramIndex   = "idx:program:"   // idx:program:{programId} -> SET of sub IDs
	keyWildcardSubs   = "idx:wildcard"   // SET of sub IDs with no specific filters (match all)
	keyExpirations    = "sub:expirations" // ZSET of sub IDs with expiration timestamps
)

// RedisConfig holds configuration for the Redis subscription manager.
type RedisConfig struct {
	// Redis client options
	Addr     string
	Password string
	DB       int

	// KeyPrefix allows namespacing keys (e.g., "prod:", "staging:")
	KeyPrefix string

	// DefaultTTL sets the default subscription expiration (0 = no expiration)
	DefaultTTL time.Duration
}

// RedisManager implements Manager using Redis for storage.
type RedisManager struct {
	client    *redis.Client
	keyPrefix string
	defaultTTL time.Duration
}

// NewRedisManager creates a new Redis-backed subscription manager.
func NewRedisManager(cfg RedisConfig) (*RedisManager, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	return &RedisManager{
		client:    client,
		keyPrefix: cfg.KeyPrefix,
		defaultTTL: cfg.DefaultTTL,
	}, nil
}

// NewRedisManagerWithClient creates a manager using an existing Redis client.
func NewRedisManagerWithClient(client *redis.Client, keyPrefix string) *RedisManager {
	return &RedisManager{
		client:    client,
		keyPrefix: keyPrefix,
	}
}

func (m *RedisManager) key(parts ...string) string {
	result := m.keyPrefix
	for _, p := range parts {
		result += p
	}
	return result
}

// Subscribe creates or updates a subscription.
func (m *RedisManager) Subscribe(ctx context.Context, sub *Subscription) (string, error) {
	if sub.ID == "" {
		sub.ID = generateSubscriptionID()
	}
	if sub.CreatedAt.IsZero() {
		sub.CreatedAt = time.Now()
	}

	// Serialize subscription
	data, err := json.Marshal(sub)
	if err != nil {
		return "", fmt.Errorf("marshal subscription: %w", err)
	}

	// Use pipeline for atomic operations
	pipe := m.client.TxPipeline()

	// Store subscription
	subKey := m.key(keySubscription, sub.ID)
	pipe.Set(ctx, subKey, data, 0)

	// Add to client's subscription set
	pipe.SAdd(ctx, m.key(keyClientSubs, sub.ClientID), sub.ID)

	// Build indexes for efficient matching
	m.addIndexes(ctx, pipe, sub)

	// Handle expiration
	if !sub.ExpiresAt.IsZero() {
		pipe.ZAdd(ctx, m.key(keyExpirations), redis.Z{
			Score:  float64(sub.ExpiresAt.Unix()),
			Member: sub.ID,
		})
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return "", fmt.Errorf("subscribe pipeline: %w", err)
	}

	return sub.ID, nil
}

func (m *RedisManager) addIndexes(ctx context.Context, pipe redis.Pipeliner, sub *Subscription) {
	f := sub.Filter

	// Track if we have any specific filters
	hasFilters := false

	// Chain indexes
	for _, chain := range f.Chains {
		pipe.SAdd(ctx, m.key(keyChainIndex, strconv.Itoa(int(chain))), sub.ID)
		hasFilters = true
	}

	// Account indexes
	for _, account := range f.Accounts {
		pipe.SAdd(ctx, m.key(keyAccountIndex, account), sub.ID)
		hasFilters = true
	}

	// Event type indexes
	for _, eventType := range f.EventTypes {
		pipe.SAdd(ctx, m.key(keyEventTypeIndex, eventType), sub.ID)
		hasFilters = true
	}

	// Program ID indexes
	for _, programId := range f.ProgramIds {
		pipe.SAdd(ctx, m.key(keyProgramIndex, programId), sub.ID)
		hasFilters = true
	}

	// If no specific filters, add to wildcard set
	if !hasFilters {
		pipe.SAdd(ctx, m.key(keyWildcardSubs), sub.ID)
	}
}

// Unsubscribe removes a subscription by ID.
func (m *RedisManager) Unsubscribe(ctx context.Context, subID string) error {
	// Get subscription first to know which indexes to remove
	sub, err := m.Get(ctx, subID)
	if err != nil {
		return err
	}
	if sub == nil {
		return nil // Already removed
	}

	return m.removeSubscription(ctx, sub)
}

func (m *RedisManager) removeSubscription(ctx context.Context, sub *Subscription) error {
	pipe := m.client.TxPipeline()

	// Remove subscription data
	pipe.Del(ctx, m.key(keySubscription, sub.ID))

	// Remove from client's set
	pipe.SRem(ctx, m.key(keyClientSubs, sub.ClientID), sub.ID)

	// Remove from indexes
	m.removeIndexes(ctx, pipe, sub)

	// Remove from expirations
	pipe.ZRem(ctx, m.key(keyExpirations), sub.ID)

	_, err := pipe.Exec(ctx)
	return err
}

func (m *RedisManager) removeIndexes(ctx context.Context, pipe redis.Pipeliner, sub *Subscription) {
	f := sub.Filter

	for _, chain := range f.Chains {
		pipe.SRem(ctx, m.key(keyChainIndex, strconv.Itoa(int(chain))), sub.ID)
	}

	for _, account := range f.Accounts {
		pipe.SRem(ctx, m.key(keyAccountIndex, account), sub.ID)
	}

	for _, eventType := range f.EventTypes {
		pipe.SRem(ctx, m.key(keyEventTypeIndex, eventType), sub.ID)
	}

	for _, programId := range f.ProgramIds {
		pipe.SRem(ctx, m.key(keyProgramIndex, programId), sub.ID)
	}

	// Also remove from wildcard set (in case it was there)
	pipe.SRem(ctx, m.key(keyWildcardSubs), sub.ID)
}

// UnsubscribeAll removes all subscriptions for a client.
func (m *RedisManager) UnsubscribeAll(ctx context.Context, clientID string) error {
	subs, err := m.ListByClient(ctx, clientID)
	if err != nil {
		return err
	}

	for _, sub := range subs {
		if err := m.removeSubscription(ctx, sub); err != nil {
			return err
		}
	}

	return nil
}

// Get retrieves a subscription by ID.
func (m *RedisManager) Get(ctx context.Context, subID string) (*Subscription, error) {
	data, err := m.client.Get(ctx, m.key(keySubscription, subID)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get subscription: %w", err)
	}

	var sub Subscription
	if err := json.Unmarshal(data, &sub); err != nil {
		return nil, fmt.Errorf("unmarshal subscription: %w", err)
	}

	return &sub, nil
}

// ListByClient returns all subscriptions for a client.
func (m *RedisManager) ListByClient(ctx context.Context, clientID string) ([]*Subscription, error) {
	subIDs, err := m.client.SMembers(ctx, m.key(keyClientSubs, clientID)).Result()
	if err != nil {
		return nil, fmt.Errorf("list client subs: %w", err)
	}

	if len(subIDs) == 0 {
		return nil, nil
	}

	// Fetch all subscriptions in a pipeline
	pipe := m.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(subIDs))
	for i, id := range subIDs {
		cmds[i] = pipe.Get(ctx, m.key(keySubscription, id))
	}
	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("fetch subscriptions: %w", err)
	}

	subs := make([]*Subscription, 0, len(subIDs))
	for _, cmd := range cmds {
		data, err := cmd.Bytes()
		if err == redis.Nil {
			continue // Subscription was deleted
		}
		if err != nil {
			return nil, err
		}

		var sub Subscription
		if err := json.Unmarshal(data, &sub); err != nil {
			return nil, fmt.Errorf("unmarshal subscription: %w", err)
		}
		subs = append(subs, &sub)
	}

	return subs, nil
}

// Match finds all subscriptions that match the given event.
func (m *RedisManager) Match(ctx context.Context, event *protov1.CanonicalEvent) ([]MatchResult, error) {
	// Get candidate subscription IDs from indexes
	candidates := make(map[string]bool)

	// Start with wildcard subscriptions (match all)
	wildcardIDs, err := m.client.SMembers(ctx, m.key(keyWildcardSubs)).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("get wildcard subs: %w", err)
	}
	for _, id := range wildcardIDs {
		candidates[id] = true
	}

	// Get chain-specific subscriptions
	chainKey := m.key(keyChainIndex, strconv.Itoa(int(event.Chain)))
	chainIDs, _ := m.client.SMembers(ctx, chainKey).Result()
	for _, id := range chainIDs {
		candidates[id] = true
	}

	// Get event-type-specific subscriptions
	if event.EventType != "" {
		typeKey := m.key(keyEventTypeIndex, event.EventType)
		typeIDs, _ := m.client.SMembers(ctx, typeKey).Result()
		for _, id := range typeIDs {
			candidates[id] = true
		}
	}

	// Get account-specific subscriptions
	for _, account := range event.Accounts {
		acctKey := m.key(keyAccountIndex, account)
		acctIDs, _ := m.client.SMembers(ctx, acctKey).Result()
		for _, id := range acctIDs {
			candidates[id] = true
		}
	}

	// Get program-specific subscriptions
	if event.ProgramId != "" {
		progKey := m.key(keyProgramIndex, event.ProgramId)
		progIDs, _ := m.client.SMembers(ctx, progKey).Result()
		for _, id := range progIDs {
			candidates[id] = true
		}
	}

	if len(candidates) == 0 {
		return nil, nil
	}

	// Fetch candidate subscriptions and do full filter matching
	candidateList := make([]string, 0, len(candidates))
	for id := range candidates {
		candidateList = append(candidateList, id)
	}

	pipe := m.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(candidateList))
	for i, id := range candidateList {
		cmds[i] = pipe.Get(ctx, m.key(keySubscription, id))
	}
	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("fetch candidate subs: %w", err)
	}

	var results []MatchResult
	now := time.Now()

	for _, cmd := range cmds {
		data, err := cmd.Bytes()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			continue
		}

		var sub Subscription
		if err := json.Unmarshal(data, &sub); err != nil {
			continue
		}

		// Check expiration
		if !sub.ExpiresAt.IsZero() && sub.ExpiresAt.Before(now) {
			continue
		}

		// Full filter match
		if sub.Filter.Matches(event) {
			results = append(results, MatchResult{
				SubscriptionID: sub.ID,
				ClientID:       sub.ClientID,
			})
		}
	}

	return results, nil
}

// MatchBatch finds matching subscriptions for multiple events.
func (m *RedisManager) MatchBatch(ctx context.Context, events []*protov1.CanonicalEvent) (map[string][]MatchResult, error) {
	results := make(map[string][]MatchResult, len(events))

	// For now, iterate over events. Can be optimized with parallel matching.
	for _, event := range events {
		matches, err := m.Match(ctx, event)
		if err != nil {
			return nil, fmt.Errorf("match event %s: %w", event.EventId, err)
		}
		results[event.EventId] = matches
	}

	return results, nil
}

// Refresh updates the expiration time for a subscription.
func (m *RedisManager) Refresh(ctx context.Context, subID string, expiresAt time.Time) error {
	sub, err := m.Get(ctx, subID)
	if err != nil {
		return err
	}
	if sub == nil {
		return fmt.Errorf("subscription not found: %s", subID)
	}

	sub.ExpiresAt = expiresAt

	// Update subscription and expiration index
	data, err := json.Marshal(sub)
	if err != nil {
		return fmt.Errorf("marshal subscription: %w", err)
	}

	pipe := m.client.TxPipeline()
	pipe.Set(ctx, m.key(keySubscription, subID), data, 0)

	if expiresAt.IsZero() {
		pipe.ZRem(ctx, m.key(keyExpirations), subID)
	} else {
		pipe.ZAdd(ctx, m.key(keyExpirations), redis.Z{
			Score:  float64(expiresAt.Unix()),
			Member: subID,
		})
	}

	_, err = pipe.Exec(ctx)
	return err
}

// Cleanup removes expired subscriptions.
func (m *RedisManager) Cleanup(ctx context.Context) (int, error) {
	now := time.Now().Unix()

	// Get expired subscription IDs
	expiredIDs, err := m.client.ZRangeByScore(ctx, m.key(keyExpirations), &redis.ZRangeBy{
		Min: "-inf",
		Max: strconv.FormatInt(now, 10),
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("get expired subs: %w", err)
	}

	if len(expiredIDs) == 0 {
		return 0, nil
	}

	count := 0
	for _, id := range expiredIDs {
		if err := m.Unsubscribe(ctx, id); err == nil {
			count++
		}
	}

	return count, nil
}

// Count returns the total number of active subscriptions.
func (m *RedisManager) Count(ctx context.Context) (int64, error) {
	// Count all subscription keys
	var cursor uint64
	var count int64

	for {
		keys, nextCursor, err := m.client.Scan(ctx, cursor, m.key(keySubscription)+"*", 1000).Result()
		if err != nil {
			return 0, fmt.Errorf("scan subscriptions: %w", err)
		}
		count += int64(len(keys))
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return count, nil
}

// Close releases any resources held by the manager.
func (m *RedisManager) Close() error {
	return m.client.Close()
}

// generateSubscriptionID creates a unique subscription ID.
func generateSubscriptionID() string {
	return fmt.Sprintf("sub_%d_%d", time.Now().UnixNano(), time.Now().Nanosecond()%1000)
}
