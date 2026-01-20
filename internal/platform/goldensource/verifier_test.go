package goldensource

import (
	"context"
	"log/slog"
	"testing"
	"time"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

// mockClient implements Client for testing.
type mockClient struct {
	name      string
	chain     protov1.Chain
	connected bool
	blocks    map[uint64]*BlockData
	latestNum uint64
}

func newMockClient(chain protov1.Chain) *mockClient {
	return &mockClient{
		name:   "mock-golden",
		chain:  chain,
		blocks: make(map[uint64]*BlockData),
	}
}

func (m *mockClient) Name() string                       { return m.name }
func (m *mockClient) Chain() protov1.Chain               { return m.chain }
func (m *mockClient) Connect(_ context.Context) error    { m.connected = true; return nil }
func (m *mockClient) Close() error                       { m.connected = false; return nil }
func (m *mockClient) IsConnected() bool                  { return m.connected }
func (m *mockClient) GetLatestBlockNumber(_ context.Context) (uint64, error) {
	return m.latestNum, nil
}

func (m *mockClient) GetBlockData(_ context.Context, blockNumber uint64) (*BlockData, error) {
	if !m.connected {
		return nil, ErrNotConnected
	}
	block, ok := m.blocks[blockNumber]
	if !ok {
		return nil, ErrBlockNotFound
	}
	return block, nil
}

func (m *mockClient) VerifyBlock(ctx context.Context, primary *BlockData) (*VerificationResult, error) {
	golden, err := m.GetBlockData(ctx, primary.BlockNumber)
	if err != nil {
		return nil, err
	}
	return Verify(primary, golden), nil
}

func (m *mockClient) addBlock(block *BlockData) {
	m.blocks[block.BlockNumber] = block
	if block.BlockNumber > m.latestNum {
		m.latestNum = block.BlockNumber
	}
}

func TestVerifier_BasicVerification(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()

	mock := newMockClient(protov1.Chain_CHAIN_ETHEREUM)
	mock.addBlock(&BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      100,
		BlockHash:        "0xabc123",
		ParentHash:       "0xdef456",
		TransactionCount: 50,
		Timestamp:        time.Now(),
	})

	verifier := NewVerifier(DefaultVerifierConfig(), logger)
	verifier.RegisterClient(mock)

	if err := mock.Connect(ctx); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	// Verify matching block
	primary := &BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      100,
		BlockHash:        "0xabc123",
		ParentHash:       "0xdef456",
		TransactionCount: 50,
		Timestamp:        time.Now(),
	}

	result, err := verifier.VerifyBlock(ctx, primary)
	if err != nil {
		t.Fatalf("verification failed: %v", err)
	}
	if !result.Verified {
		t.Errorf("expected verification to pass: %v", result.Errors)
	}

	stats := verifier.Stats()
	if stats.BlocksVerified != 1 {
		t.Errorf("expected 1 block verified, got %d", stats.BlocksVerified)
	}
	if stats.BlocksPassed != 1 {
		t.Errorf("expected 1 block passed, got %d", stats.BlocksPassed)
	}
}

func TestVerifier_FailClosed(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()

	mock := newMockClient(protov1.Chain_CHAIN_ETHEREUM)
	mock.addBlock(&BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      100,
		BlockHash:        "0xGOLDEN_HASH",
		ParentHash:       "0xdef456",
		TransactionCount: 50,
	})

	cfg := DefaultVerifierConfig()
	cfg.FailClosed = true
	verifier := NewVerifier(cfg, logger)
	verifier.RegisterClient(mock)

	if err := mock.Connect(ctx); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	// Verify mismatching block
	primary := &BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      100,
		BlockHash:        "0xDIFFERENT_HASH",
		ParentHash:       "0xdef456",
		TransactionCount: 50,
	}

	result, err := verifier.VerifyBlock(ctx, primary)
	if err == nil {
		t.Error("expected error for fail-closed verification failure")
	}
	if result.Verified {
		t.Error("expected verification to fail")
	}

	// Verify that verifier is now halted
	if !verifier.IsHalted() {
		t.Error("expected verifier to be halted")
	}
	if verifier.HaltReason() == "" {
		t.Error("expected halt reason to be set")
	}

	// Subsequent verifications should fail immediately
	_, err = verifier.VerifyBlock(ctx, primary)
	if err == nil {
		t.Error("expected error for verification while halted")
	}

	stats := verifier.Stats()
	if stats.BlocksFailed != 1 {
		t.Errorf("expected 1 block failed, got %d", stats.BlocksFailed)
	}
}

func TestVerifier_ResolveFailure(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()

	mock := newMockClient(protov1.Chain_CHAIN_ETHEREUM)
	mock.addBlock(&BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      100,
		BlockHash:        "0xGOLDEN",
		ParentHash:       "0xdef456",
		TransactionCount: 50,
	})

	cfg := DefaultVerifierConfig()
	cfg.FailClosed = true
	verifier := NewVerifier(cfg, logger)
	verifier.RegisterClient(mock)
	mock.Connect(ctx)

	// Trigger failure
	primary := &BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      100,
		BlockHash:        "0xDIFFERENT",
		ParentHash:       "0xdef456",
		TransactionCount: 50,
	}
	verifier.VerifyBlock(ctx, primary)

	if !verifier.IsHalted() {
		t.Error("expected verifier to be halted")
	}

	// Resolve failure
	if err := verifier.ResolveFailure("issue investigated and fixed"); err != nil {
		t.Errorf("failed to resolve: %v", err)
	}

	if verifier.IsHalted() {
		t.Error("expected verifier to be resumed")
	}

	// Now update the golden source with correct data and verify again
	mock.blocks[100].BlockHash = "0xCORRECT"
	primary.BlockHash = "0xCORRECT"

	result, err := verifier.VerifyBlock(ctx, primary)
	if err != nil {
		t.Fatalf("verification should work after resolve: %v", err)
	}
	if !result.Verified {
		t.Errorf("verification should pass with matching data: %v", result.Errors)
	}
}

func TestVerifier_SampledVerification(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()

	mock := newMockClient(protov1.Chain_CHAIN_ETHEREUM)
	for i := uint64(100); i <= 110; i++ {
		mock.addBlock(&BlockData{
			Chain:       protov1.Chain_CHAIN_ETHEREUM,
			BlockNumber: i,
			BlockHash:   "0xhash",
			ParentHash:  "0xparent",
		})
	}

	cfg := DefaultVerifierConfig()
	cfg.VerifyEveryNthBlock = 5 // Only verify every 5th block
	verifier := NewVerifier(cfg, logger)
	verifier.RegisterClient(mock)
	mock.Connect(ctx)

	// Verify blocks 100-110
	for i := uint64(100); i <= 110; i++ {
		primary := &BlockData{
			Chain:       protov1.Chain_CHAIN_ETHEREUM,
			BlockNumber: i,
			BlockHash:   "0xhash",
			ParentHash:  "0xparent",
		}
		verifier.VerifyBlock(ctx, primary)
	}

	stats := verifier.Stats()
	// Blocks 100, 105, 110 should be verified (multiples of 5)
	if stats.BlocksVerified != 3 {
		t.Errorf("expected 3 blocks verified, got %d", stats.BlocksVerified)
	}
	if stats.BlocksSkipped != 8 {
		t.Errorf("expected 8 blocks skipped, got %d", stats.BlocksSkipped)
	}
}

func TestVerifier_Callback(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()

	mock := newMockClient(protov1.Chain_CHAIN_ETHEREUM)
	mock.addBlock(&BlockData{
		Chain:       protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber: 100,
		BlockHash:   "0xabc",
		ParentHash:  "0xdef",
	})

	verifier := NewVerifier(DefaultVerifierConfig(), logger)
	verifier.RegisterClient(mock)
	mock.Connect(ctx)

	callbackCalled := false
	var callbackResult *VerificationResult
	verifier.OnVerification(func(_ context.Context, result *VerificationResult) error {
		callbackCalled = true
		callbackResult = result
		return nil
	})

	primary := &BlockData{
		Chain:       protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber: 100,
		BlockHash:   "0xabc",
		ParentHash:  "0xdef",
	}
	verifier.VerifyBlock(ctx, primary)

	if !callbackCalled {
		t.Error("expected callback to be called")
	}
	if callbackResult == nil || !callbackResult.Verified {
		t.Error("expected callback to receive verified result")
	}
}

func TestDefaultVerifierConfig(t *testing.T) {
	cfg := DefaultVerifierConfig()

	if !cfg.FailClosed {
		t.Error("expected FailClosed to be true by default")
	}
	if cfg.VerifyEveryNthBlock != 1 {
		t.Errorf("expected VerifyEveryNthBlock to be 1, got %d", cfg.VerifyEveryNthBlock)
	}
	if cfg.SkipIfGoldenSourceUnavailable {
		t.Error("expected SkipIfGoldenSourceUnavailable to be false by default")
	}
}
