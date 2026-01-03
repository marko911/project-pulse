package subscription

import (
	"testing"
	"time"

	protov1 "github.com/mirador/pulse/pkg/proto/v1"
)

func TestFilter_Matches(t *testing.T) {
	tests := []struct {
		name   string
		filter Filter
		event  *protov1.CanonicalEvent
		want   bool
	}{
		{
			name:   "empty filter matches everything",
			filter: Filter{},
			event: &protov1.CanonicalEvent{
				EventId:         "evt1",
				Chain:           protov1.Chain_CHAIN_ETHEREUM,
				EventType:       "transfer",
				CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
				Accounts:        []string{"0x123", "0x456"},
			},
			want: true,
		},
		{
			name: "chain filter matches",
			filter: Filter{
				Chains: []protov1.Chain{protov1.Chain_CHAIN_ETHEREUM},
			},
			event: &protov1.CanonicalEvent{
				Chain: protov1.Chain_CHAIN_ETHEREUM,
			},
			want: true,
		},
		{
			name: "chain filter does not match",
			filter: Filter{
				Chains: []protov1.Chain{protov1.Chain_CHAIN_SOLANA},
			},
			event: &protov1.CanonicalEvent{
				Chain: protov1.Chain_CHAIN_ETHEREUM,
			},
			want: false,
		},
		{
			name: "event type filter matches",
			filter: Filter{
				EventTypes: []string{"transfer", "swap"},
			},
			event: &protov1.CanonicalEvent{
				EventType: "transfer",
			},
			want: true,
		},
		{
			name: "event type filter does not match",
			filter: Filter{
				EventTypes: []string{"mint", "burn"},
			},
			event: &protov1.CanonicalEvent{
				EventType: "transfer",
			},
			want: false,
		},
		{
			name: "commitment level filter matches",
			filter: Filter{
				CommitmentLevels: []protov1.CommitmentLevel{
					protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
				},
			},
			event: &protov1.CanonicalEvent{
				CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
			},
			want: true,
		},
		{
			name: "commitment level filter does not match",
			filter: Filter{
				CommitmentLevels: []protov1.CommitmentLevel{
					protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
				},
			},
			event: &protov1.CanonicalEvent{
				CommitmentLevel: protov1.CommitmentLevel_COMMITMENT_LEVEL_CONFIRMED,
			},
			want: false,
		},
		{
			name: "account filter matches any",
			filter: Filter{
				Accounts: []string{"0x123"},
			},
			event: &protov1.CanonicalEvent{
				Accounts: []string{"0x456", "0x123", "0x789"},
			},
			want: true,
		},
		{
			name: "account filter does not match",
			filter: Filter{
				Accounts: []string{"0xabc"},
			},
			event: &protov1.CanonicalEvent{
				Accounts: []string{"0x123", "0x456"},
			},
			want: false,
		},
		{
			name: "program ID filter matches",
			filter: Filter{
				ProgramIds: []string{"TokenProgram"},
			},
			event: &protov1.CanonicalEvent{
				ProgramId: "TokenProgram",
			},
			want: true,
		},
		{
			name: "min native value filter matches",
			filter: Filter{
				MinNativeValue: 1000,
			},
			event: &protov1.CanonicalEvent{
				NativeValue: 5000,
			},
			want: true,
		},
		{
			name: "min native value filter does not match",
			filter: Filter{
				MinNativeValue: 10000,
			},
			event: &protov1.CanonicalEvent{
				NativeValue: 5000,
			},
			want: false,
		},
		{
			name: "combined filters all match",
			filter: Filter{
				Chains:     []protov1.Chain{protov1.Chain_CHAIN_ETHEREUM, protov1.Chain_CHAIN_POLYGON},
				EventTypes: []string{"transfer"},
				Accounts:   []string{"0x123"},
			},
			event: &protov1.CanonicalEvent{
				Chain:     protov1.Chain_CHAIN_ETHEREUM,
				EventType: "transfer",
				Accounts:  []string{"0x123", "0x456"},
			},
			want: true,
		},
		{
			name: "combined filters partial match fails",
			filter: Filter{
				Chains:     []protov1.Chain{protov1.Chain_CHAIN_ETHEREUM},
				EventTypes: []string{"swap"}, // doesn't match
			},
			event: &protov1.CanonicalEvent{
				Chain:     protov1.Chain_CHAIN_ETHEREUM,
				EventType: "transfer",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.filter.Matches(tt.event)
			if got != tt.want {
				t.Errorf("Filter.Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSubscription_Fields(t *testing.T) {
	now := time.Now()
	expires := now.Add(time.Hour)

	sub := Subscription{
		ID:       "sub_123",
		ClientID: "client_abc",
		Filter: Filter{
			Chains:     []protov1.Chain{protov1.Chain_CHAIN_SOLANA},
			EventTypes: []string{"transfer"},
		},
		CreatedAt: now,
		ExpiresAt: expires,
		Metadata: map[string]string{
			"user_id": "user_123",
		},
	}

	if sub.ID != "sub_123" {
		t.Errorf("unexpected ID: %s", sub.ID)
	}
	if sub.ClientID != "client_abc" {
		t.Errorf("unexpected ClientID: %s", sub.ClientID)
	}
	if len(sub.Filter.Chains) != 1 || sub.Filter.Chains[0] != protov1.Chain_CHAIN_SOLANA {
		t.Errorf("unexpected chains: %v", sub.Filter.Chains)
	}
	if sub.Metadata["user_id"] != "user_123" {
		t.Errorf("unexpected metadata: %v", sub.Metadata)
	}
}
