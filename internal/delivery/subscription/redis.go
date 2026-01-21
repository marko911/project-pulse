package subscription

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

const (
	keySubscription   = "sub:"
	keyClientSubs     = "client:subs:"
	keyChainIndex     = "idx:chain:"
	keyAccountIndex   = "idx:account:"
	keyEventTypeIndex = "idx:eventtype:"
	keyProgramIndex   = "idx:program:"
	keyWildcardSubs   = "idx:wildcard"
	keyExpirations    = "sub:expirations"
)

type RedisConfig struct {
	Addr     string
	Password string
	DB       int

	KeyPrefix string

	DefaultTTL time.Duration
}

type RedisManager struct {
	client    *redis.Client
	keyPrefix string
	defaultTTL time.Duration
}

func NewRedisManager(cfg RedisConfig) (*RedisManager, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

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

func (m *RedisManager) Subscribe(ctx context.Context, sub *Subscription) (string, error) {
	if sub.ID == "" {
		sub.ID = generateSubscriptionID()
	}
	if sub.CreatedAt.IsZero() {
		sub.CreatedAt = time.Now()
	}

	data, err := json.Marshal(sub)
	if err != nil {
		return "", fmt.Errorf("marshal subscription: %w", err)
	}

	pipe := m.client.TxPipeline()

	subKey := m.key(keySubscription, sub.ID)
	pipe.Set(ctx, subKey, data, 0)

	pipe.SAdd(ctx, m.key(keyClientSubs, sub.ClientID), sub.ID)

	m.addIndexes(ctx, pipe, sub)

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

	hasFilters := false

	for _, chain := range f.Chains {
		pipe.SAdd(ctx, m.key(keyChainIndex, strconv.Itoa(int(chain))), sub.ID)
		hasFilters = true
	}

	for _, account := range f.Accounts {
		pipe.SAdd(ctx, m.key(keyAccountIndex, account), sub.ID)
		hasFilters = true
	}

	for _, eventType := range f.EventTypes {
		pipe.SAdd(ctx, m.key(keyEventTypeIndex, eventType), sub.ID)
		hasFilters = true
	}

	for _, programId := range f.ProgramIds {
		pipe.SAdd(ctx, m.key(keyProgramIndex, programId), sub.ID)
		hasFilters = true
	}

	if !hasFilters {
		pipe.SAdd(ctx, m.key(keyWildcardSubs), sub.ID)
	}
}

func (m *RedisManager) Unsubscribe(ctx context.Context, subID string) error {
	sub, err := m.Get(ctx, subID)
	if err != nil {
		return err
	}
	if sub == nil {
		return nil
	}

	return m.removeSubscription(ctx, sub)
}

func (m *RedisManager) removeSubscription(ctx context.Context, sub *Subscription) error {
	pipe := m.client.TxPipeline()

	pipe.Del(ctx, m.key(keySubscription, sub.ID))

	pipe.SRem(ctx, m.key(keyClientSubs, sub.ClientID), sub.ID)

	m.removeIndexes(ctx, pipe, sub)

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

	pipe.SRem(ctx, m.key(keyWildcardSubs), sub.ID)
}

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

func (m *RedisManager) ListByClient(ctx context.Context, clientID string) ([]*Subscription, error) {
	subIDs, err := m.client.SMembers(ctx, m.key(keyClientSubs, clientID)).Result()
	if err != nil {
		return nil, fmt.Errorf("list client subs: %w", err)
	}

	if len(subIDs) == 0 {
		return nil, nil
	}

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
			continue
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

func (m *RedisManager) Match(ctx context.Context, event *protov1.CanonicalEvent) ([]MatchResult, error) {
	candidates := make(map[string]bool)

	wildcardIDs, err := m.client.SMembers(ctx, m.key(keyWildcardSubs)).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("get wildcard subs: %w", err)
	}
	for _, id := range wildcardIDs {
		candidates[id] = true
	}

	chainKey := m.key(keyChainIndex, strconv.Itoa(int(event.Chain)))
	chainIDs, _ := m.client.SMembers(ctx, chainKey).Result()
	for _, id := range chainIDs {
		candidates[id] = true
	}

	if event.EventType != "" {
		typeKey := m.key(keyEventTypeIndex, event.EventType)
		typeIDs, _ := m.client.SMembers(ctx, typeKey).Result()
		for _, id := range typeIDs {
			candidates[id] = true
		}
	}

	for _, account := range event.Accounts {
		acctKey := m.key(keyAccountIndex, account)
		acctIDs, _ := m.client.SMembers(ctx, acctKey).Result()
		for _, id := range acctIDs {
			candidates[id] = true
		}
	}

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

		if !sub.ExpiresAt.IsZero() && sub.ExpiresAt.Before(now) {
			continue
		}

		if sub.Filter.Matches(event) {
			results = append(results, MatchResult{
				SubscriptionID: sub.ID,
				ClientID:       sub.ClientID,
			})
		}
	}

	return results, nil
}

func (m *RedisManager) FindMatchingSubscribers(ctx context.Context, event *protov1.CanonicalEvent) ([]string, error) {
	matches, err := m.Match(ctx, event)
	if err != nil {
		return nil, err
	}

	if len(matches) == 0 {
		return nil, nil
	}

	seen := make(map[string]bool, len(matches))
	clientIDs := make([]string, 0, len(matches))
	for _, match := range matches {
		if !seen[match.ClientID] {
			seen[match.ClientID] = true
			clientIDs = append(clientIDs, match.ClientID)
		}
	}

	return clientIDs, nil
}

func (m *RedisManager) MatchBatch(ctx context.Context, events []*protov1.CanonicalEvent) (map[string][]MatchResult, error) {
	if len(events) == 0 {
		return make(map[string][]MatchResult), nil
	}

	if len(events) == 1 {
		matches, err := m.Match(ctx, events[0])
		if err != nil {
			return nil, fmt.Errorf("match event %s: %w", events[0].EventId, err)
		}
		return map[string][]MatchResult{events[0].EventId: matches}, nil
	}

	const maxWorkers = 8
	numWorkers := len(events)
	if numWorkers > maxWorkers {
		numWorkers = maxWorkers
	}

	type matchJob struct {
		event *protov1.CanonicalEvent
	}
	type matchResult struct {
		eventID string
		matches []MatchResult
		err     error
	}

	jobs := make(chan matchJob, len(events))
	resultsCh := make(chan matchResult, len(events))

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				matches, err := m.Match(ctx, job.event)
				resultsCh <- matchResult{
					eventID: job.event.EventId,
					matches: matches,
					err:     err,
				}
			}
		}()
	}

	for _, event := range events {
		jobs <- matchJob{event: event}
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	results := make(map[string][]MatchResult, len(events))
	var firstErr error
	for res := range resultsCh {
		if res.err != nil && firstErr == nil {
			firstErr = fmt.Errorf("match event %s: %w", res.eventID, res.err)
		}
		results[res.eventID] = res.matches
	}

	if firstErr != nil {
		return nil, firstErr
	}

	return results, nil
}

func (m *RedisManager) Refresh(ctx context.Context, subID string, expiresAt time.Time) error {
	sub, err := m.Get(ctx, subID)
	if err != nil {
		return err
	}
	if sub == nil {
		return fmt.Errorf("subscription not found: %s", subID)
	}

	sub.ExpiresAt = expiresAt

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

func (m *RedisManager) Cleanup(ctx context.Context) (int, error) {
	now := time.Now().Unix()

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

func (m *RedisManager) Count(ctx context.Context) (int64, error) {
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

func (m *RedisManager) Close() error {
	return m.client.Close()
}

func generateSubscriptionID() string {
	return fmt.Sprintf("sub_%d_%d", time.Now().UnixNano(), time.Now().Nanosecond()%1000)
}
