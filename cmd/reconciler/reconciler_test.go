package main

import (
	"context"
	"testing"
	"time"

	"github.com/mirador/pulse/internal/platform/goldensource"
	protov1 "github.com/mirador/pulse/pkg/proto/v1"
)

// mockGoldenClient implements goldensource.Client for testing
type mockGoldenClient struct {
	name       string
	chain      protov1.Chain
	connected  bool
	blockData  map[uint64]*goldensource.BlockData
	verifyFunc func(primary *goldensource.BlockData) (*goldensource.VerificationResult, error)
}

func newMockGoldenClient(chain protov1.Chain) *mockGoldenClient {
	return &mockGoldenClient{
		name:      "mock-golden",
		chain:     chain,
		connected: false,
		blockData: make(map[uint64]*goldensource.BlockData),
	}
}

func (m *mockGoldenClient) Name() string                    { return m.name }
func (m *mockGoldenClient) Chain() protov1.Chain            { return m.chain }
func (m *mockGoldenClient) Connect(ctx context.Context) error { m.connected = true; return nil }
func (m *mockGoldenClient) Close() error                    { m.connected = false; return nil }
func (m *mockGoldenClient) IsConnected() bool               { return m.connected }

func (m *mockGoldenClient) GetBlockData(ctx context.Context, blockNumber uint64) (*goldensource.BlockData, error) {
	if data, ok := m.blockData[blockNumber]; ok {
		return data, nil
	}
	return nil, goldensource.ErrBlockNotFound
}

func (m *mockGoldenClient) GetLatestBlockNumber(ctx context.Context) (uint64, error) {
	var max uint64
	for bn := range m.blockData {
		if bn > max {
			max = bn
		}
	}
	return max, nil
}

func (m *mockGoldenClient) VerifyBlock(ctx context.Context, primary *goldensource.BlockData) (*goldensource.VerificationResult, error) {
	if m.verifyFunc != nil {
		return m.verifyFunc(primary)
	}

	golden, err := m.GetBlockData(ctx, primary.BlockNumber)
	if err != nil {
		return nil, err
	}

	return goldensource.Verify(primary, golden), nil
}

func (m *mockGoldenClient) AddBlockData(data *goldensource.BlockData) {
	m.blockData[data.BlockNumber] = data
}

func TestReconcilerConfig_Defaults(t *testing.T) {
	cfg := DefaultReconcilerConfig()

	if cfg.ReconcileInterval != 30*time.Second {
		t.Errorf("expected ReconcileInterval 30s, got %v", cfg.ReconcileInterval)
	}
	if cfg.BatchSize != 100 {
		t.Errorf("expected BatchSize 100, got %d", cfg.BatchSize)
	}
	if cfg.LookbackBlocks != 1000 {
		t.Errorf("expected LookbackBlocks 1000, got %d", cfg.LookbackBlocks)
	}
	if !cfg.FailClosed {
		t.Error("expected FailClosed true")
	}
}

func TestReconciliationResult_Match(t *testing.T) {
	result := &ReconciliationResult{
		Chain:       protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber: 12345,
		BlockHash:   "0xabc",
		Matched:     true,
		Errors:      nil,
	}

	if !result.Matched {
		t.Error("expected result to be matched")
	}
	if len(result.Errors) != 0 {
		t.Errorf("expected no errors, got %v", result.Errors)
	}
}

func TestReconciliationResult_Mismatch(t *testing.T) {
	result := &ReconciliationResult{
		Chain:       protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber: 12345,
		BlockHash:   "0xabc",
		Matched:     false,
		Errors:      []string{"hash mismatch", "tx count mismatch"},
	}

	if result.Matched {
		t.Error("expected result to not be matched")
	}
	if len(result.Errors) != 2 {
		t.Errorf("expected 2 errors, got %d", len(result.Errors))
	}
}

func TestReconcilerStats_Initial(t *testing.T) {
	stats := &ReconcilerStats{}

	if stats.BlocksReconciled != 0 {
		t.Errorf("expected BlocksReconciled 0, got %d", stats.BlocksReconciled)
	}
	if stats.BlocksMatched != 0 {
		t.Errorf("expected BlocksMatched 0, got %d", stats.BlocksMatched)
	}
	if stats.BlocksMismatched != 0 {
		t.Errorf("expected BlocksMismatched 0, got %d", stats.BlocksMismatched)
	}
}

func TestMockGoldenClient_VerifyMatch(t *testing.T) {
	client := newMockGoldenClient(protov1.Chain_CHAIN_ETHEREUM)
	client.Connect(context.Background())

	// Add matching block data
	blockData := &goldensource.BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      12345,
		BlockHash:        "0xabc123",
		ParentHash:       "0xdef456",
		TransactionCount: 100,
		Timestamp:        time.Now(),
	}
	client.AddBlockData(blockData)

	// Verify with matching primary data
	primary := &goldensource.BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      12345,
		BlockHash:        "0xabc123",
		ParentHash:       "0xdef456",
		TransactionCount: 100,
	}

	result, err := client.VerifyBlock(context.Background(), primary)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Verified {
		t.Errorf("expected verification to pass, errors: %v", result.Errors)
	}
}

func TestMockGoldenClient_VerifyHashMismatch(t *testing.T) {
	client := newMockGoldenClient(protov1.Chain_CHAIN_ETHEREUM)
	client.Connect(context.Background())

	// Add golden block data
	goldenData := &goldensource.BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      12345,
		BlockHash:        "0xgolden",
		ParentHash:       "0xparent",
		TransactionCount: 100,
	}
	client.AddBlockData(goldenData)

	// Verify with different hash
	primary := &goldensource.BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      12345,
		BlockHash:        "0xprimary", // Different!
		ParentHash:       "0xparent",
		TransactionCount: 100,
	}

	result, err := client.VerifyBlock(context.Background(), primary)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Verified {
		t.Error("expected verification to fail due to hash mismatch")
	}

	hasHashError := false
	for _, e := range result.Errors {
		if e == goldensource.ErrHashMismatch {
			hasHashError = true
			break
		}
	}
	if !hasHashError {
		t.Errorf("expected ErrHashMismatch in errors: %v", result.Errors)
	}
}

func TestMockGoldenClient_VerifyTxCountMismatch(t *testing.T) {
	client := newMockGoldenClient(protov1.Chain_CHAIN_ETHEREUM)
	client.Connect(context.Background())

	// Add golden block data
	goldenData := &goldensource.BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      12345,
		BlockHash:        "0xhash",
		ParentHash:       "0xparent",
		TransactionCount: 100,
	}
	client.AddBlockData(goldenData)

	// Verify with different tx count
	primary := &goldensource.BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      12345,
		BlockHash:        "0xhash",
		ParentHash:       "0xparent",
		TransactionCount: 99, // Different!
	}

	result, err := client.VerifyBlock(context.Background(), primary)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Verified {
		t.Error("expected verification to fail due to tx count mismatch")
	}

	hasTxError := false
	for _, e := range result.Errors {
		if e == goldensource.ErrTxCountDrift {
			hasTxError = true
			break
		}
	}
	if !hasTxError {
		t.Errorf("expected ErrTxCountDrift in errors: %v", result.Errors)
	}
}

func TestMockGoldenClient_BlockNotFound(t *testing.T) {
	client := newMockGoldenClient(protov1.Chain_CHAIN_ETHEREUM)
	client.Connect(context.Background())

	// Don't add any block data

	primary := &goldensource.BlockData{
		Chain:       protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber: 99999,
	}

	_, err := client.VerifyBlock(context.Background(), primary)
	if err == nil {
		t.Error("expected error for missing block")
	}
	if err != goldensource.ErrBlockNotFound {
		t.Errorf("expected ErrBlockNotFound, got %v", err)
	}
}

func TestVerifier_FailClosed(t *testing.T) {
	cfg := goldensource.DefaultVerifierConfig()
	cfg.FailClosed = true

	verifier := goldensource.NewVerifier(cfg, nil)

	// Register mock client
	client := newMockGoldenClient(protov1.Chain_CHAIN_ETHEREUM)
	client.Connect(context.Background())

	// Add mismatched golden data
	goldenData := &goldensource.BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      12345,
		BlockHash:        "0xgolden",
		ParentHash:       "0xparent",
		TransactionCount: 100,
	}
	client.AddBlockData(goldenData)
	verifier.RegisterClient(client)

	// Verify with mismatched primary
	primary := &goldensource.BlockData{
		Chain:            protov1.Chain_CHAIN_ETHEREUM,
		BlockNumber:      12345,
		BlockHash:        "0xprimary", // Mismatch!
		ParentHash:       "0xparent",
		TransactionCount: 100,
	}

	_, err := verifier.VerifyBlock(context.Background(), primary)
	if err == nil {
		t.Error("expected error from fail-closed behavior")
	}

	if !verifier.IsHalted() {
		t.Error("expected verifier to be halted")
	}
}
