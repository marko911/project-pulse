package processor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/marko911/project-pulse/internal/adapter"
	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

type EVMNormalizer struct {
	chainID protov1.Chain
}

func NewEVMNormalizer(chainName string) *EVMNormalizer {
	return &EVMNormalizer{
		chainID: mapEVMChain(chainName),
	}
}

func (n *EVMNormalizer) Chain() string {
	return chainNameFromProto(n.chainID)
}

func (n *EVMNormalizer) Normalize(ctx context.Context, event adapter.Event) (*protov1.CanonicalEvent, error) {
	chainID := mapEVMChain(event.Chain)
	if chainID == protov1.Chain_CHAIN_UNSPECIFIED || chainID == protov1.Chain_CHAIN_SOLANA {
		return nil, fmt.Errorf("evm normalizer received non-evm event: %s", event.Chain)
	}

	eventID := n.generateEventID(event)

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
		Accounts:        event.Accounts,
		Timestamp:       time.Unix(event.Timestamp, 0),
		Payload:         event.Payload,
		ReorgAction:     protov1.ReorgAction_REORG_ACTION_NORMAL,
		SchemaVersion:   1,
		IngestedAt:      time.Now(),
		ProgramId:       event.ProgramID,
		NativeValue:     event.NativeValue,
	}, nil
}

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
