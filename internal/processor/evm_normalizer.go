package processor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/mirador/pulse/internal/adapter"
	protov1 "github.com/mirador/pulse/pkg/proto/v1"
)

// EVMNormalizer converts EVM adapter events to canonical format.
type EVMNormalizer struct {
	chainID protov1.Chain
}

// NewEVMNormalizer creates a new EVM normalizer for the specified chain.
func NewEVMNormalizer(chainName string) *EVMNormalizer {
	return &EVMNormalizer{
		chainID: mapEVMChain(chainName),
	}
}

// Chain returns the chain identifier.
func (n *EVMNormalizer) Chain() string {
	return chainNameFromProto(n.chainID)
}

// Normalize converts an EVM adapter event to canonical protobuf format.
func (n *EVMNormalizer) Normalize(ctx context.Context, event adapter.Event) (*protov1.CanonicalEvent, error) {
	// Validate this is an EVM chain event
	chainID := mapEVMChain(event.Chain)
	if chainID == protov1.Chain_CHAIN_UNSPECIFIED || chainID == protov1.Chain_CHAIN_SOLANA {
		return nil, fmt.Errorf("evm normalizer received non-evm event: %s", event.Chain)
	}

	// Generate deterministic event ID
	eventID := n.generateEventID(event)

	// Map commitment level (EVM uses different terminology)
	commitment := n.mapCommitmentLevel(event.CommitmentLevel)

	return &protov1.CanonicalEvent{
		EventId:         eventID,
		Chain:           chainID,
		CommitmentLevel: commitment,
		BlockNumber:     event.BlockNumber,
		BlockHash:       event.BlockHash,
		TxHash:          event.TxHash,
		TxIndex:         event.TxIndex,
		EventIndex:      event.EventIndex,
		EventType:       event.EventType,
		Accounts:        event.Accounts, // Contract addresses involved
		Timestamp:       time.Unix(event.Timestamp, 0),
		Payload:         event.Payload, // ABI-encoded log data
		ReorgAction:     protov1.ReorgAction_REORG_ACTION_NORMAL,
		SchemaVersion:   1,
		IngestedAt:      time.Now(),
		ProgramId:       event.ProgramID, // Contract address for EVM
		NativeValue:     event.NativeValue,
	}, nil
}

// generateEventID creates a deterministic ID from event fields.
func (n *EVMNormalizer) generateEventID(event adapter.Event) string {
	data := fmt.Sprintf("%s:%d:%s:%d:%d",
		event.Chain,
		event.BlockNumber,
		event.TxHash,
		event.TxIndex,
		event.EventIndex,
	)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:16])
}

// mapCommitmentLevel converts EVM confirmation terminology to canonical.
func (n *EVMNormalizer) mapCommitmentLevel(level string) protov1.CommitmentLevel {
	switch level {
	case "pending", "latest":
		return protov1.CommitmentLevel_COMMITMENT_LEVEL_PROCESSED
	case "safe":
		return protov1.CommitmentLevel_COMMITMENT_LEVEL_CONFIRMED
	case "finalized":
		return protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED
	default:
		return protov1.CommitmentLevel_COMMITMENT_LEVEL_UNSPECIFIED
	}
}

// mapEVMChain converts chain name to protobuf enum.
func mapEVMChain(name string) protov1.Chain {
	switch name {
	case "ethereum", "eth", "mainnet":
		return protov1.Chain_CHAIN_ETHEREUM
	case "polygon", "matic":
		return protov1.Chain_CHAIN_POLYGON
	case "arbitrum", "arb":
		return protov1.Chain_CHAIN_ARBITRUM
	case "optimism", "op":
		return protov1.Chain_CHAIN_OPTIMISM
	case "base":
		return protov1.Chain_CHAIN_BASE
	case "avalanche", "avax":
		return protov1.Chain_CHAIN_AVALANCHE
	case "bsc", "binance":
		return protov1.Chain_CHAIN_BSC
	default:
		return protov1.Chain_CHAIN_UNSPECIFIED
	}
}

// chainNameFromProto converts protobuf enum to chain name.
func chainNameFromProto(chain protov1.Chain) string {
	switch chain {
	case protov1.Chain_CHAIN_ETHEREUM:
		return "ethereum"
	case protov1.Chain_CHAIN_POLYGON:
		return "polygon"
	case protov1.Chain_CHAIN_ARBITRUM:
		return "arbitrum"
	case protov1.Chain_CHAIN_OPTIMISM:
		return "optimism"
	case protov1.Chain_CHAIN_BASE:
		return "base"
	case protov1.Chain_CHAIN_AVALANCHE:
		return "avalanche"
	case protov1.Chain_CHAIN_BSC:
		return "bsc"
	case protov1.Chain_CHAIN_SOLANA:
		return "solana"
	default:
		return "unknown"
	}
}
