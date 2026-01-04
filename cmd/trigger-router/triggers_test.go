package main

import (
	"testing"

	protov1 "github.com/mirador/pulse/pkg/proto/v1"
)

func TestTriggerFilter_Matches(t *testing.T) {
	tests := []struct {
		name     string
		filter   TriggerFilter
		event    *protov1.CanonicalEvent
		expected bool
	}{
		{
			name:   "empty filter matches all",
			filter: TriggerFilter{},
			event: &protov1.CanonicalEvent{
				Chain:     protov1.Chain_CHAIN_ETHEREUM,
				EventType: "Transfer",
				Accounts:  []string{"0x123"},
			},
			expected: true,
		},
		{
			name: "chain filter matches",
			filter: TriggerFilter{
				Chains: []protov1.Chain{protov1.Chain_CHAIN_ETHEREUM},
			},
			event: &protov1.CanonicalEvent{
				Chain:     protov1.Chain_CHAIN_ETHEREUM,
				EventType: "Transfer",
			},
			expected: true,
		},
		{
			name: "chain filter does not match",
			filter: TriggerFilter{
				Chains: []protov1.Chain{protov1.Chain_CHAIN_SOLANA},
			},
			event: &protov1.CanonicalEvent{
				Chain:     protov1.Chain_CHAIN_ETHEREUM,
				EventType: "Transfer",
			},
			expected: false,
		},
		{
			name: "event type filter matches",
			filter: TriggerFilter{
				EventTypes: []string{"Transfer", "Approval"},
			},
			event: &protov1.CanonicalEvent{
				Chain:     protov1.Chain_CHAIN_ETHEREUM,
				EventType: "Transfer",
			},
			expected: true,
		},
		{
			name: "event type filter does not match",
			filter: TriggerFilter{
				EventTypes: []string{"Approval"},
			},
			event: &protov1.CanonicalEvent{
				Chain:     protov1.Chain_CHAIN_ETHEREUM,
				EventType: "Transfer",
			},
			expected: false,
		},
		{
			name: "account filter matches",
			filter: TriggerFilter{
				Accounts: []string{"0xabc"},
			},
			event: &protov1.CanonicalEvent{
				Chain:    protov1.Chain_CHAIN_ETHEREUM,
				Accounts: []string{"0xabc", "0xdef"},
			},
			expected: true,
		},
		{
			name: "account filter does not match",
			filter: TriggerFilter{
				Accounts: []string{"0x999"},
			},
			event: &protov1.CanonicalEvent{
				Chain:    protov1.Chain_CHAIN_ETHEREUM,
				Accounts: []string{"0xabc", "0xdef"},
			},
			expected: false,
		},
		{
			name: "program id filter matches",
			filter: TriggerFilter{
				ProgramIDs: []string{"TokenProgram111"},
			},
			event: &protov1.CanonicalEvent{
				Chain:     protov1.Chain_CHAIN_SOLANA,
				ProgramId: "TokenProgram111",
			},
			expected: true,
		},
		{
			name: "program id filter does not match",
			filter: TriggerFilter{
				ProgramIDs: []string{"OtherProgram"},
			},
			event: &protov1.CanonicalEvent{
				Chain:     protov1.Chain_CHAIN_SOLANA,
				ProgramId: "TokenProgram111",
			},
			expected: false,
		},
		{
			name: "multiple filters all match",
			filter: TriggerFilter{
				Chains:     []protov1.Chain{protov1.Chain_CHAIN_ETHEREUM},
				EventTypes: []string{"Transfer"},
				Accounts:   []string{"0xabc"},
			},
			event: &protov1.CanonicalEvent{
				Chain:     protov1.Chain_CHAIN_ETHEREUM,
				EventType: "Transfer",
				Accounts:  []string{"0xabc", "0xdef"},
			},
			expected: true,
		},
		{
			name: "multiple filters partial match fails",
			filter: TriggerFilter{
				Chains:     []protov1.Chain{protov1.Chain_CHAIN_ETHEREUM},
				EventTypes: []string{"Approval"}, // This doesn't match
				Accounts:   []string{"0xabc"},
			},
			event: &protov1.CanonicalEvent{
				Chain:     protov1.Chain_CHAIN_ETHEREUM,
				EventType: "Transfer",
				Accounts:  []string{"0xabc", "0xdef"},
			},
			expected: false,
		},
		{
			name: "contract filter matches via accounts",
			filter: TriggerFilter{
				Contracts: []string{"0xcontract"},
			},
			event: &protov1.CanonicalEvent{
				Chain:    protov1.Chain_CHAIN_ETHEREUM,
				Accounts: []string{"0xcontract", "0xuser"},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.filter.Matches(tt.event)
			if result != tt.expected {
				t.Errorf("Matches() = %v, want %v", result, tt.expected)
			}
		})
	}
}
