package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

const (
	keyTrigger        = "trigger:"
	keyTenantTriggers = "tenant:triggers:"
	keyFuncTriggers   = "func:triggers:"
	keyChainTriggers  = "idx:chain:"
	keyEventTriggers  = "idx:event:"
	keyAccountTriggers= "idx:account:"
	keyProgramTriggers= "idx:program:"
	keyAllTriggers    = "all_triggers"
)

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

type TriggerFilter struct {
	Chains []protov1.Chain `json:"chains,omitempty"`

	EventTypes []string `json:"event_types,omitempty"`

	Accounts []string `json:"accounts,omitempty"`

	ProgramIDs []string `json:"program_ids,omitempty"`

	Contracts []string `json:"contracts,omitempty"`
}

func (f *TriggerFilter) Matches(event *protov1.CanonicalEvent) bool {
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

type FunctionInvocation struct {
	InvocationID string                   `json:"invocation_id"`
	FunctionID   string                   `json:"function_id"`
	TriggerID    string                   `json:"trigger_id"`
	TenantID     string                   `json:"tenant_id"`
	Event        *protov1.CanonicalEvent  `json:"event"`
	CreatedAt    time.Time                `json:"created_at"`
}

type TriggerManager struct {
	client    *redis.Client
	keyPrefix string
}

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

	pipe.Set(ctx, m.key(keyTrigger, trigger.ID), data, 0)

	pipe.SAdd(ctx, m.key(keyTenantTriggers, trigger.TenantID), trigger.ID)

	pipe.SAdd(ctx, m.key(keyFuncTriggers, trigger.FunctionID), trigger.ID)

	pipe.SAdd(ctx, m.key(keyAllTriggers), trigger.ID)

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

func (m *TriggerManager) Delete(ctx context.Context, id string) error {
	trigger, err := m.Get(ctx, id)
	if err != nil {
		return err
	}
	if trigger == nil {
		return nil
	}

	pipe := m.client.TxPipeline()

	pipe.Del(ctx, m.key(keyTrigger, id))

	pipe.SRem(ctx, m.key(keyTenantTriggers, trigger.TenantID), id)
	pipe.SRem(ctx, m.key(keyFuncTriggers, trigger.FunctionID), id)
	pipe.SRem(ctx, m.key(keyAllTriggers), id)

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

func (m *TriggerManager) ListByTenant(ctx context.Context, tenantID string) ([]*Trigger, error) {
	ids, err := m.client.SMembers(ctx, m.key(keyTenantTriggers, tenantID)).Result()
	if err != nil {
		return nil, fmt.Errorf("list tenant triggers: %w", err)
	}

	return m.fetchTriggers(ctx, ids)
}

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

func (m *TriggerManager) Match(ctx context.Context, event *protov1.CanonicalEvent) ([]*Trigger, error) {
	candidates := make(map[string]bool)

	chainKey := m.key(keyChainTriggers, strconv.Itoa(int(event.Chain)))
	chainIDs, _ := m.client.SMembers(ctx, chainKey).Result()
	for _, id := range chainIDs {
		candidates[id] = true
	}

	if event.EventType != "" {
		typeKey := m.key(keyEventTriggers, event.EventType)
		typeIDs, _ := m.client.SMembers(ctx, typeKey).Result()
		for _, id := range typeIDs {
			candidates[id] = true
		}
	}

	for _, account := range event.Accounts {
		acctKey := m.key(keyAccountTriggers, account)
		acctIDs, _ := m.client.SMembers(ctx, acctKey).Result()
		for _, id := range acctIDs {
			candidates[id] = true
		}
	}

	if event.ProgramId != "" {
		progKey := m.key(keyProgramTriggers, event.ProgramId)
		progIDs, _ := m.client.SMembers(ctx, progKey).Result()
		for _, id := range progIDs {
			candidates[id] = true
		}
	}

	allIDs, _ := m.client.SMembers(ctx, m.key(keyAllTriggers)).Result()
	for _, id := range allIDs {
		candidates[id] = true
	}

	if len(candidates) == 0 {
		return nil, nil
	}

	candidateList := make([]string, 0, len(candidates))
	for id := range candidates {
		candidateList = append(candidateList, id)
	}

	triggers, err := m.fetchTriggers(ctx, candidateList)
	if err != nil {
		return nil, err
	}

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

func generateTriggerID() string {
	return fmt.Sprintf("trg_%d_%d", time.Now().UnixNano(), time.Now().Nanosecond()%1000)
}
