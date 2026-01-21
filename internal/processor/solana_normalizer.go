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

type SolanaNormalizer struct{}

func NewSolanaNormalizer() *SolanaNormalizer {
	return &SolanaNormalizer{}
}

func (n *SolanaNormalizer) Chain() string {
	return "solana"
}

func (n *SolanaNormalizer) Normalize(ctx context.Context, event adapter.Event) (*protov1.CanonicalEvent, error) {
	if event.Chain != "solana" {
		return nil, fmt.Errorf("solana normalizer received non-solana event: %s", event.Chain)
	}

	eventID := n.generateEventID(event)

	commitment := n.mapCommitmentLevel(event.CommitmentLevel)

	return &protov1.CanonicalEvent{
		EventId:         eventID,
		Chain:           protov1.Chain_CHAIN_SOLANA,
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

func (n *SolanaNormalizer) generateEventID(event adapter.Event) string {
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

func (n *SolanaNormalizer) mapCommitmentLevel(level string) protov1.CommitmentLevel {
	switch level {
	case "processed":
		return protov1.CommitmentLevel_COMMITMENT_LEVEL_PROCESSED
	case "confirmed":
		return protov1.CommitmentLevel_COMMITMENT_LEVEL_CONFIRMED
	case "finalized":
		return protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED
	default:
		return protov1.CommitmentLevel_COMMITMENT_LEVEL_UNSPECIFIED
	}
}
