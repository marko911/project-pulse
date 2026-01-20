package goldensource

import (
	"testing"
	"time"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

func TestVerify_MatchingBlocks(t *testing.T) {
	now := time.Now()
	primary := &BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      12345,
		BlockHash:        "0xabcdef1234567890",
		ParentHash:       "0x1234567890abcdef",
		TransactionCount: 150,
		Timestamp:        now,
		FetchedAt:        now,
	}

	golden := &BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      12345,
		BlockHash:        "0xabcdef1234567890",
		ParentHash:       "0x1234567890abcdef",
		TransactionCount: 150,
		Timestamp:        now,
		FetchedAt:        now,
	}

	result := Verify(primary, golden)

	if !result.Verified {
		t.Errorf("expected verification to pass, got errors: %v", result.Errors)
	}
	if len(result.Errors) != 0 {
		t.Errorf("expected no errors, got %d", len(result.Errors))
	}
	if result.BlockNumber != 12345 {
		t.Errorf("expected block number 12345, got %d", result.BlockNumber)
	}
}

func TestVerify_HashMismatch(t *testing.T) {
	now := time.Now()
	primary := &BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      12345,
		BlockHash:        "0xabcdef1234567890",
		ParentHash:       "0x1234567890abcdef",
		TransactionCount: 150,
		Timestamp:        now,
	}

	golden := &BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      12345,
		BlockHash:        "0xDIFFERENT_HASH",
		ParentHash:       "0x1234567890abcdef",
		TransactionCount: 150,
		Timestamp:        now,
	}

	result := Verify(primary, golden)

	if result.Verified {
		t.Error("expected verification to fail due to hash mismatch")
	}
	if len(result.Errors) != 1 {
		t.Errorf("expected 1 error, got %d", len(result.Errors))
	}
	if result.Errors[0] != ErrHashMismatch {
		t.Errorf("expected ErrHashMismatch, got %v", result.Errors[0])
	}
}

func TestVerify_ParentHashMismatch(t *testing.T) {
	now := time.Now()
	primary := &BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      12345,
		BlockHash:        "0xabcdef1234567890",
		ParentHash:       "0x1234567890abcdef",
		TransactionCount: 150,
		Timestamp:        now,
	}

	golden := &BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      12345,
		BlockHash:        "0xabcdef1234567890",
		ParentHash:       "0xDIFFERENT_PARENT",
		TransactionCount: 150,
		Timestamp:        now,
	}

	result := Verify(primary, golden)

	if result.Verified {
		t.Error("expected verification to fail due to parent hash mismatch")
	}
	if len(result.Errors) != 1 {
		t.Errorf("expected 1 error, got %d", len(result.Errors))
	}
	if result.Errors[0] != ErrParentMismatch {
		t.Errorf("expected ErrParentMismatch, got %v", result.Errors[0])
	}
}

func TestVerify_TxCountDrift(t *testing.T) {
	now := time.Now()
	primary := &BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      12345,
		BlockHash:        "0xabcdef1234567890",
		ParentHash:       "0x1234567890abcdef",
		TransactionCount: 150,
		Timestamp:        now,
	}

	golden := &BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      12345,
		BlockHash:        "0xabcdef1234567890",
		ParentHash:       "0x1234567890abcdef",
		TransactionCount: 155, // Different tx count
		Timestamp:        now,
	}

	result := Verify(primary, golden)

	if result.Verified {
		t.Error("expected verification to fail due to tx count drift")
	}
	if len(result.Errors) != 1 {
		t.Errorf("expected 1 error, got %d", len(result.Errors))
	}
	if result.Errors[0] != ErrTxCountDrift {
		t.Errorf("expected ErrTxCountDrift, got %v", result.Errors[0])
	}
}

func TestVerify_ChainMismatch(t *testing.T) {
	now := time.Now()
	primary := &BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      12345,
		BlockHash:        "0xabcdef1234567890",
		ParentHash:       "0x1234567890abcdef",
		TransactionCount: 150,
		Timestamp:        now,
	}

	golden := &BlockData{
		Chain:            protov1.Chain_CHAIN_POLYGON, // Different chain!
		BlockNumber:      12345,
		BlockHash:        "0xabcdef1234567890",
		ParentHash:       "0x1234567890abcdef",
		TransactionCount: 150,
		Timestamp:        now,
	}

	result := Verify(primary, golden)

	if result.Verified {
		t.Error("expected verification to fail due to chain mismatch")
	}
	foundChainMismatch := false
	for _, err := range result.Errors {
		if err == ErrChainMismatch {
			foundChainMismatch = true
			break
		}
	}
	if !foundChainMismatch {
		t.Error("expected ErrChainMismatch in errors")
	}
}

func TestVerify_MultipleErrors(t *testing.T) {
	now := time.Now()
	primary := &BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      12345,
		BlockHash:        "0xabcdef1234567890",
		ParentHash:       "0x1234567890abcdef",
		TransactionCount: 150,
		Timestamp:        now,
	}

	golden := &BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      12345,
		BlockHash:        "0xDIFFERENT",
		ParentHash:       "0xDIFFERENT_PARENT",
		TransactionCount: 200,
		Timestamp:        now,
	}

	result := Verify(primary, golden)

	if result.Verified {
		t.Error("expected verification to fail")
	}
	if len(result.Errors) != 3 {
		t.Errorf("expected 3 errors, got %d: %v", len(result.Errors), result.Errors)
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Timeout != 10*time.Second {
		t.Errorf("expected 10s timeout, got %v", cfg.Timeout)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("expected 3 max retries, got %d", cfg.MaxRetries)
	}
	if cfg.RetryInterval != 1*time.Second {
		t.Errorf("expected 1s retry interval, got %v", cfg.RetryInterval)
	}
}
