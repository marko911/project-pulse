package processor

import (
	"context"
	"testing"
	"time"

	"github.com/mirador/pulse/internal/adapter"
	protov1 "github.com/mirador/pulse/pkg/proto/v1"
)

func TestSolanaNormalizer_Normalize(t *testing.T) {
	n := NewSolanaNormalizer()

	tests := []struct {
		name           string
		event          adapter.Event
		wantChain      protov1.Chain
		wantCommitment protov1.CommitmentLevel
		wantErr        bool
	}{
		{
			name: "confirmed solana event",
			event: adapter.Event{
				Chain:           "solana",
				CommitmentLevel: "confirmed",
				BlockNumber:     12345,
				BlockHash:       "abc123",
				TxHash:          "tx456",
				TxIndex:         0,
				EventIndex:      1,
				EventType:       "transfer",
				Accounts:        []string{"account1", "account2"},
				Timestamp:       time.Now().Unix(),
				ProgramID:       "TokenProgram",
			},
			wantChain:      protov1.Chain_CHAIN_SOLANA,
			wantCommitment: protov1.CommitmentLevel_COMMITMENT_LEVEL_CONFIRMED,
			wantErr:        false,
		},
		{
			name: "finalized solana event",
			event: adapter.Event{
				Chain:           "solana",
				CommitmentLevel: "finalized",
				BlockNumber:     12346,
				BlockHash:       "def456",
				TxHash:          "tx789",
			},
			wantChain:      protov1.Chain_CHAIN_SOLANA,
			wantCommitment: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
			wantErr:        false,
		},
		{
			name: "non-solana event should fail",
			event: adapter.Event{
				Chain: "ethereum",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := n.Normalize(context.Background(), tt.event)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if result.Chain != tt.wantChain {
				t.Errorf("chain = %v, want %v", result.Chain, tt.wantChain)
			}

			if result.CommitmentLevel != tt.wantCommitment {
				t.Errorf("commitment = %v, want %v", result.CommitmentLevel, tt.wantCommitment)
			}

			if result.EventId == "" {
				t.Error("expected non-empty event ID")
			}

			if result.BlockNumber != tt.event.BlockNumber {
				t.Errorf("block number = %d, want %d", result.BlockNumber, tt.event.BlockNumber)
			}
		})
	}
}

func TestEVMNormalizer_Normalize(t *testing.T) {
	n := NewEVMNormalizer("ethereum")

	tests := []struct {
		name           string
		event          adapter.Event
		wantChain      protov1.Chain
		wantCommitment protov1.CommitmentLevel
		wantErr        bool
	}{
		{
			name: "ethereum finalized event",
			event: adapter.Event{
				Chain:           "ethereum",
				CommitmentLevel: "finalized",
				BlockNumber:     18000000,
				BlockHash:       "0xabc123",
				TxHash:          "0xtx456",
				TxIndex:         5,
				EventIndex:      2,
				EventType:       "Transfer",
				Accounts:        []string{"0xfrom", "0xto"},
				Timestamp:       time.Now().Unix(),
				ProgramID:       "0xcontract",
			},
			wantChain:      protov1.Chain_CHAIN_ETHEREUM,
			wantCommitment: protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED,
			wantErr:        false,
		},
		{
			name: "polygon safe event",
			event: adapter.Event{
				Chain:           "polygon",
				CommitmentLevel: "safe",
				BlockNumber:     50000000,
				BlockHash:       "0xpoly123",
				TxHash:          "0xpolytx",
			},
			wantChain:      protov1.Chain_CHAIN_POLYGON,
			wantCommitment: protov1.CommitmentLevel_COMMITMENT_LEVEL_CONFIRMED,
			wantErr:        false,
		},
		{
			name: "arbitrum latest event",
			event: adapter.Event{
				Chain:           "arbitrum",
				CommitmentLevel: "latest",
				BlockNumber:     100000000,
			},
			wantChain:      protov1.Chain_CHAIN_ARBITRUM,
			wantCommitment: protov1.CommitmentLevel_COMMITMENT_LEVEL_PROCESSED,
			wantErr:        false,
		},
		{
			name: "solana event should fail",
			event: adapter.Event{
				Chain: "solana",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := n.Normalize(context.Background(), tt.event)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if result.Chain != tt.wantChain {
				t.Errorf("chain = %v, want %v", result.Chain, tt.wantChain)
			}

			if result.CommitmentLevel != tt.wantCommitment {
				t.Errorf("commitment = %v, want %v", result.CommitmentLevel, tt.wantCommitment)
			}

			if result.EventId == "" {
				t.Error("expected non-empty event ID")
			}
		})
	}
}

func TestSolanaNormalizer_Chain(t *testing.T) {
	n := NewSolanaNormalizer()
	if got := n.Chain(); got != "solana" {
		t.Errorf("Chain() = %v, want solana", got)
	}
}

func TestEVMNormalizer_Chain(t *testing.T) {
	tests := []struct {
		chainName string
		want      string
	}{
		{"ethereum", "ethereum"},
		{"polygon", "polygon"},
		{"arbitrum", "arbitrum"},
		{"base", "base"},
	}

	for _, tt := range tests {
		t.Run(tt.chainName, func(t *testing.T) {
			n := NewEVMNormalizer(tt.chainName)
			if got := n.Chain(); got != tt.want {
				t.Errorf("Chain() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEventIDDeterminism(t *testing.T) {
	n := NewSolanaNormalizer()

	event := adapter.Event{
		Chain:       "solana",
		BlockNumber: 12345,
		TxHash:      "tx123",
		TxIndex:     0,
		EventIndex:  1,
	}

	result1, _ := n.Normalize(context.Background(), event)
	result2, _ := n.Normalize(context.Background(), event)

	if result1.EventId != result2.EventId {
		t.Errorf("event IDs should be deterministic: %s != %s", result1.EventId, result2.EventId)
	}
}
