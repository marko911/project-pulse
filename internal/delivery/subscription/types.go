package subscription

import (
	"context"
	"time"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

type Filter struct {
	Chains []protov1.Chain `json:"chains,omitempty"`

	EventTypes []string `json:"event_types,omitempty"`

	Accounts []string `json:"accounts,omitempty"`

	ProgramIds []string `json:"program_ids,omitempty"`

	CommitmentLevels []protov1.CommitmentLevel `json:"commitment_levels,omitempty"`

	MinNativeValue uint64 `json:"min_native_value,omitempty"`
}

type Subscription struct {
	ID string `json:"id"`

	ClientID string `json:"client_id"`

	Filter Filter `json:"filter"`

	CreatedAt time.Time `json:"created_at"`

	ExpiresAt time.Time `json:"expires_at,omitempty"`

	Metadata map[string]string `json:"metadata,omitempty"`
}

type Manager interface {
	Subscribe(ctx context.Context, sub *Subscription) (string, error)

	Unsubscribe(ctx context.Context, subID string) error

	UnsubscribeAll(ctx context.Context, clientID string) error

	Get(ctx context.Context, subID string) (*Subscription, error)

	ListByClient(ctx context.Context, clientID string) ([]*Subscription, error)

	Match(ctx context.Context, event *protov1.CanonicalEvent) ([]MatchResult, error)

	MatchBatch(ctx context.Context, events []*protov1.CanonicalEvent) (map[string][]MatchResult, error)

	Refresh(ctx context.Context, subID string, expiresAt time.Time) error

	Cleanup(ctx context.Context) (int, error)

	Count(ctx context.Context) (int64, error)

	Close() error
}

type MatchResult struct {
	SubscriptionID string
	ClientID       string
}

func (f *Filter) Matches(event *protov1.CanonicalEvent) bool {
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

	if f.MinNativeValue > 0 && event.NativeValue < f.MinNativeValue {
		return false
	}

	return true
}
