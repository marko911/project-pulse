package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	protov1 "github.com/mirador/pulse/pkg/proto/v1"
)

// Redis key patterns for triggers
const (
	keyTrigger        = "trigger:"           // trigger:{id} -> Trigger JSON
	keyTenantTriggers = "tenant:triggers:"   // tenant:triggers:{tenantID} -> SET of trigger IDs
	keyFuncTriggers   = "func:triggers:"     // func:triggers:{funcID} -> SET of trigger IDs
	keyChainTriggers  = "idx:chain:"         // idx:chain:{chain} -> SET of trigger IDs
	keyEventTriggers  = "idx:event:"         // idx:event:{eventType} -> SET of trigger IDs
	keyAccountTriggers= "idx:account:"       // idx:account:{account} -> SET of trigger IDs
	keyProgramTriggers= "idx:program:"       // idx:program:{programId} -> SET of trigger IDs
	keyAllTriggers    = "all_triggers"       // SET of all active trigger IDs
)

// Trigger represents a user-defined event trigger.
type Trigger struct {
	ID          string          `json:"id"`
	TenantID    string          `json:"tenant_id"`
	FunctionID  string          `json:"function_id"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Filter      TriggerFilter   `json:"filter"`
	Enabled     bool            `json:"enabled"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// TriggerFilter defines the event matching criteria.
type TriggerFilter struct {
	// Chain filter (empty = all chains)
	Chains []protov1.Chain `json:"chains,omitempty"`

	// Event type filter (empty = all types)
	EventTypes []string `json:"event_types,omitempty"`

	// Account filter (empty = all accounts)
	Accounts []string `json:"accounts,omitempty"`

	// Program ID filter (for Solana, empty = all programs)
	ProgramIDs []string `json:"program_ids,omitempty"`

	// Contract address filter (for EVM, empty = all contracts)
	Contracts []string `json:"contracts,omitempty"`
}

// Matches checks if an event matches this filter.
func (f *TriggerFilter) Matches(event *protov1.CanonicalEvent) bool {
	// Chain filter
	if len(f.Chains) > 0 {
		matched := false
		for _, chain := range f.Chains {
			if chain == event.Chain {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Event type filter
	if len(f.EventTypes) > 0 {
		matched := false
		for _, et := range f.EventTypes {
			if event.EventType == et {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Account filter
	if len(f.Accounts) > 0 {
		matched := false
		for _, filterAccount := range f.Accounts {
			for _, eventAccount := range event.Accounts {
				if filterAccount == eventAccount {
					matched = true
					break
				}
			}
			if matched {
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Program ID filter
	if len(f.ProgramIDs) > 0 {
		matched := false
		for _, pid := range f.ProgramIDs {
			if event.ProgramId == pid {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Contract filter (stored in accounts for EVM events)
	if len(f.Contracts) > 0 {
		matched := false
		for _, contract := range f.Contracts {
			for _, eventAccount := range event.Accounts {
				if contract == eventAccount {
					matched = true
					break
				}
			}
			if matched {
				break
			}
		}
		if !matched {
			return false
		}
	}

	return true
}

// FunctionInvocation represents a request to invoke a WASM function.
type FunctionInvocation struct {
	InvocationID string                   `json:"invocation_id"`
	FunctionID   string                   `json:"function_id"`
	TriggerID    string                   `json:"trigger_id"`
	TenantID     string                   `json:"tenant_id"`
	Event        *protov1.CanonicalEvent  `json:"event"`
	CreatedAt    time.Time                `json:"created_at"`
}

// TriggerManager manages triggers stored in Redis.
type TriggerManager struct {
	client    *redis.Client
	keyPrefix string
}

// NewTriggerManager creates a new TriggerManager.
func NewTriggerManager(client *redis.Client, prefix string) *TriggerManager {
	return &TriggerManager{
		client:    client,
		keyPrefix: prefix,
	}
}

func (m *TriggerManager) key(parts ...string) string {
	result := m.keyPrefix
	for _, p := range parts {
		result += p
	}
	return result
}

// Create stores a new trigger.
func (m *TriggerManager) Create(ctx context.Context, trigger *Trigger) error {
	if trigger.ID == "" {
		trigger.ID = generateTriggerID()
	}
	now := time.Now()
	trigger.CreatedAt = now
	trigger.UpdatedAt = now

	data, err := json.Marshal(trigger)
	if err != nil {
		return fmt.Errorf("marshal trigger: %w", err)
	}

	pipe := m.client.TxPipeline()

	// Store trigger
	pipe.Set(ctx, m.key(keyTrigger, trigger.ID), data, 0)

	// Index by tenant
	pipe.SAdd(ctx, m.key(keyTenantTriggers, trigger.TenantID), trigger.ID)

	// Index by function
	pipe.SAdd(ctx, m.key(keyFuncTriggers, trigger.FunctionID), trigger.ID)

	// Add to all triggers set
	pipe.SAdd(ctx, m.key(keyAllTriggers), trigger.ID)

	// Build filter indexes
	m.addFilterIndexes(ctx, pipe, trigger)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("create trigger: %w", err)
	}

	return nil
}

func (m *TriggerManager) addFilterIndexes(ctx context.Context, pipe redis.Pipeliner, trigger *Trigger) {
	f := trigger.Filter

	for _, chain := range f.Chains {
		pipe.SAdd(ctx, m.key(keyChainTriggers, strconv.Itoa(int(chain))), trigger.ID)
	}

	for _, eventType := range f.EventTypes {
		pipe.SAdd(ctx, m.key(keyEventTriggers, eventType), trigger.ID)
	}

	for _, account := range f.Accounts {
		pipe.SAdd(ctx, m.key(keyAccountTriggers, account), trigger.ID)
	}

	for _, programID := range f.ProgramIDs {
		pipe.SAdd(ctx, m.key(keyProgramTriggers, programID), trigger.ID)
	}

	for _, contract := range f.Contracts {
		pipe.SAdd(ctx, m.key(keyAccountTriggers, contract), trigger.ID)
	}
}

// Get retrieves a trigger by ID.
func (m *TriggerManager) Get(ctx context.Context, id string) (*Trigger, error) {
	data, err := m.client.Get(ctx, m.key(keyTrigger, id)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get trigger: %w", err)
	}

	var trigger Trigger
	if err := json.Unmarshal(data, &trigger); err != nil {
		return nil, fmt.Errorf("unmarshal trigger: %w", err)
	}

	return &trigger, nil
}

// Delete removes a trigger.
func (m *TriggerManager) Delete(ctx context.Context, id string) error {
	trigger, err := m.Get(ctx, id)
	if err != nil {
		return err
	}
	if trigger == nil {
		return nil
	}

	pipe := m.client.TxPipeline()

	// Remove trigger data
	pipe.Del(ctx, m.key(keyTrigger, id))

	// Remove from indexes
	pipe.SRem(ctx, m.key(keyTenantTriggers, trigger.TenantID), id)
	pipe.SRem(ctx, m.key(keyFuncTriggers, trigger.FunctionID), id)
	pipe.SRem(ctx, m.key(keyAllTriggers), id)

	// Remove from filter indexes
	m.removeFilterIndexes(ctx, pipe, trigger)

	_, err = pipe.Exec(ctx)
	return err
}

func (m *TriggerManager) removeFilterIndexes(ctx context.Context, pipe redis.Pipeliner, trigger *Trigger) {
	f := trigger.Filter

	for _, chain := range f.Chains {
		pipe.SRem(ctx, m.key(keyChainTriggers, strconv.Itoa(int(chain))), trigger.ID)
	}

	for _, eventType := range f.EventTypes {
		pipe.SRem(ctx, m.key(keyEventTriggers, eventType), trigger.ID)
	}

	for _, account := range f.Accounts {
		pipe.SRem(ctx, m.key(keyAccountTriggers, account), trigger.ID)
	}

	for _, programID := range f.ProgramIDs {
		pipe.SRem(ctx, m.key(keyProgramTriggers, programID), trigger.ID)
	}

	for _, contract := range f.Contracts {
		pipe.SRem(ctx, m.key(keyAccountTriggers, contract), trigger.ID)
	}
}

// ListByTenant returns all triggers for a tenant.
func (m *TriggerManager) ListByTenant(ctx context.Context, tenantID string) ([]*Trigger, error) {
	ids, err := m.client.SMembers(ctx, m.key(keyTenantTriggers, tenantID)).Result()
	if err != nil {
		return nil, fmt.Errorf("list tenant triggers: %w", err)
	}

	return m.fetchTriggers(ctx, ids)
}

// ListByFunction returns all triggers for a function.
func (m *TriggerManager) ListByFunction(ctx context.Context, functionID string) ([]*Trigger, error) {
	ids, err := m.client.SMembers(ctx, m.key(keyFuncTriggers, functionID)).Result()
	if err != nil {
		return nil, fmt.Errorf("list function triggers: %w", err)
	}

	return m.fetchTriggers(ctx, ids)
}

func (m *TriggerManager) fetchTriggers(ctx context.Context, ids []string) ([]*Trigger, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	pipe := m.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.Get(ctx, m.key(keyTrigger, id))
	}
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("fetch triggers: %w", err)
	}

	triggers := make([]*Trigger, 0, len(ids))
	for _, cmd := range cmds {
		data, err := cmd.Bytes()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			continue
		}

		var trigger Trigger
		if err := json.Unmarshal(data, &trigger); err != nil {
			continue
		}
		triggers = append(triggers, &trigger)
	}

	return triggers, nil
}

// Match finds all triggers that match the given event.
func (m *TriggerManager) Match(ctx context.Context, event *protov1.CanonicalEvent) ([]*Trigger, error) {
	// Gather candidate trigger IDs from indexes
	candidates := make(map[string]bool)

	// Get triggers for this chain
	chainKey := m.key(keyChainTriggers, strconv.Itoa(int(event.Chain)))
	chainIDs, _ := m.client.SMembers(ctx, chainKey).Result()
	for _, id := range chainIDs {
		candidates[id] = true
	}

	// Get triggers for this event type
	if event.EventType != "" {
		typeKey := m.key(keyEventTriggers, event.EventType)
		typeIDs, _ := m.client.SMembers(ctx, typeKey).Result()
		for _, id := range typeIDs {
			candidates[id] = true
		}
	}

	// Get triggers for involved accounts
	for _, account := range event.Accounts {
		acctKey := m.key(keyAccountTriggers, account)
		acctIDs, _ := m.client.SMembers(ctx, acctKey).Result()
		for _, id := range acctIDs {
			candidates[id] = true
		}
	}

	// Get triggers for program ID
	if event.ProgramId != "" {
		progKey := m.key(keyProgramTriggers, event.ProgramId)
		progIDs, _ := m.client.SMembers(ctx, progKey).Result()
		for _, id := range progIDs {
			candidates[id] = true
		}
	}

	// Also consider triggers with no specific filters (match all)
	allIDs, _ := m.client.SMembers(ctx, m.key(keyAllTriggers)).Result()
	for _, id := range allIDs {
		candidates[id] = true
	}

	if len(candidates) == 0 {
		return nil, nil
	}

	// Fetch and verify candidates
	candidateList := make([]string, 0, len(candidates))
	for id := range candidates {
		candidateList = append(candidateList, id)
	}

	triggers, err := m.fetchTriggers(ctx, candidateList)
	if err != nil {
		return nil, err
	}

	// Full filter matching
	var matches []*Trigger
	for _, trigger := range triggers {
		if !trigger.Enabled {
			continue
		}
		if trigger.Filter.Matches(event) {
			matches = append(matches, trigger)
		}
	}

	return matches, nil
}

// generateTriggerID creates a unique trigger ID.
func generateTriggerID() string {
	return fmt.Sprintf("trg_%d_%d", time.Now().UnixNano(), time.Now().Nanosecond()%1000)
}
