// Package subscription provides subscription management for client interest sets.
// Subscriptions are stored in Redis for durability across gateway restarts.
package subscription

import (
	"context"
	"time"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

// Filter defines the criteria for matching events to a subscription.
// All non-empty fields must match (AND logic).
// Empty slices/fields act as wildcards (match all).
type Filter struct {
	// Chains to subscribe to. Empty means all chains.
	Chains []protov1.Chain `json:"chains,omitempty"`

	// EventTypes to match (e.g., "transfer", "swap", "mint").
	// Empty means all event types.
	EventTypes []string `json:"event_types,omitempty"`

	// Accounts to filter on (any match within the event's accounts).
	// Empty means all accounts.
	Accounts []string `json:"accounts,omitempty"`

	// ProgramIds to filter on (e.g., Solana program addresses).
	// Empty means all programs.
	ProgramIds []string `json:"program_ids,omitempty"`

	// CommitmentLevels to receive events for.
	// Empty means all commitment levels.
	CommitmentLevels []protov1.CommitmentLevel `json:"commitment_levels,omitempty"`

	// MinNativeValue filters events by minimum native token value.
	// Zero means no minimum.
	MinNativeValue uint64 `json:"min_native_value,omitempty"`
}

// Subscription represents a client's interest set.
type Subscription struct {
	// ID is the unique identifier for this subscription.
	ID string `json:"id"`

	// ClientID identifies the client/connection owning this subscription.
	ClientID string `json:"client_id"`

	// Filter defines what events this subscription matches.
	Filter Filter `json:"filter"`

	// CreatedAt is when this subscription was created.
	CreatedAt time.Time `json:"created_at"`

	// ExpiresAt optionally sets when this subscription should be auto-removed.
	// Zero time means no expiration.
	ExpiresAt time.Time `json:"expires_at,omitempty"`

	// Metadata holds optional client-provided metadata.
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Manager defines the interface for subscription storage and matching.
type Manager interface {
	// Subscribe creates or updates a subscription.
	// Returns the subscription ID.
	Subscribe(ctx context.Context, sub *Subscription) (string, error)

	// Unsubscribe removes a subscription by ID.
	Unsubscribe(ctx context.Context, subID string) error

	// UnsubscribeAll removes all subscriptions for a client.
	UnsubscribeAll(ctx context.Context, clientID string) error

	// Get retrieves a subscription by ID.
	Get(ctx context.Context, subID string) (*Subscription, error)

	// ListByClient returns all subscriptions for a client.
	ListByClient(ctx context.Context, clientID string) ([]*Subscription, error)

	// Match finds all subscriptions that match the given event.
	// Returns subscription IDs and their client IDs.
	Match(ctx context.Context, event *protov1.CanonicalEvent) ([]MatchResult, error)

	// MatchBatch finds matching subscriptions for multiple events.
	// Returns a map of event ID to matching results.
	MatchBatch(ctx context.Context, events []*protov1.CanonicalEvent) (map[string][]MatchResult, error)

	// Refresh updates the expiration time for a subscription.
	Refresh(ctx context.Context, subID string, expiresAt time.Time) error

	// Cleanup removes expired subscriptions.
	// Returns the number of subscriptions removed.
	Cleanup(ctx context.Context) (int, error)

	// Count returns the total number of active subscriptions.
	Count(ctx context.Context) (int64, error)

	// Close releases any resources held by the manager.
	Close() error
}

// MatchResult contains information about a matching subscription.
type MatchResult struct {
	SubscriptionID string
	ClientID       string
}

// Matches checks if the filter matches the given event.
func (f *Filter) Matches(event *protov1.CanonicalEvent) bool {
	// Check chains
	if len(f.Chains) > 0 {
		found := false
		for _, c := range f.Chains {
			if c == event.Chain {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Check event types
	if len(f.EventTypes) > 0 {
		found := false
		for _, t := range f.EventTypes {
			if t == event.EventType {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Check commitment levels
	if len(f.CommitmentLevels) > 0 {
		found := false
		for _, cl := range f.CommitmentLevels {
			if cl == event.CommitmentLevel {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Check accounts (any account in filter matches any account in event)
	if len(f.Accounts) > 0 {
		found := false
		for _, filterAcct := range f.Accounts {
			for _, eventAcct := range event.Accounts {
				if filterAcct == eventAcct {
					found = true
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			return false
		}
	}

	// Check program IDs
	if len(f.ProgramIds) > 0 {
		found := false
		for _, pid := range f.ProgramIds {
			if pid == event.ProgramId {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Check minimum native value
	if f.MinNativeValue > 0 && event.NativeValue < f.MinNativeValue {
		return false
	}

	return true
}
