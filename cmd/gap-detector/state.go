package main

import (
	"sync"
	"time"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

// ChainState tracks the last seen block for each chain/commitment combination.
// It is thread-safe and designed for concurrent access.
type ChainState struct {
	mu     sync.RWMutex
	chains map[chainStateKey]*BlockTracker
}

// chainStateKey uniquely identifies a chain + commitment level.
type chainStateKey struct {
	Chain           protov1.Chain
	CommitmentLevel int16
}

// BlockTracker tracks block sequence for a single chain/commitment.
type BlockTracker struct {
	LastBlock   uint64    `json:"last_block"`
	FirstBlock  uint64    `json:"first_block"`
	BlockCount  int64     `json:"block_count"`
	GapCount    int64     `json:"gap_count"`
	LastUpdated time.Time `json:"last_updated"`
	Initialized bool      `json:"initialized"`
}

// ChainStateSnapshot represents the current state for JSON serialization.
type ChainStateSnapshot struct {
	Chain           string        `json:"chain"`
	CommitmentLevel int16         `json:"commitment_level"`
	Tracker         *BlockTracker `json:"tracker"`
}

// NewChainState creates a new chain state tracker.
func NewChainState() *ChainState {
	return &ChainState{
		chains: make(map[chainStateKey]*BlockTracker),
	}
}

// CheckAndUpdate checks if there's a gap and updates the state.
// Returns a GapEvent if a gap is detected, nil otherwise.
func (s *ChainState) CheckAndUpdate(chain protov1.Chain, commitmentLevel int16, blockNumber uint64, topic string) *GapEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := chainStateKey{
		Chain:           chain,
		CommitmentLevel: commitmentLevel,
	}

	tracker, exists := s.chains[key]
	if !exists {
		// First block for this chain/commitment - initialize
		s.chains[key] = &BlockTracker{
			LastBlock:   blockNumber,
			FirstBlock:  blockNumber,
			BlockCount:  1,
			LastUpdated: time.Now(),
			Initialized: true,
		}
		return nil
	}

	// Calculate expected block (last + 1)
	expectedBlock := tracker.LastBlock + 1

	// Check for gap (received block is ahead of expected)
	if blockNumber > expectedBlock {
		gapSize := blockNumber - expectedBlock
		tracker.GapCount++

		gap := &GapEvent{
			Chain:           chain,
			CommitmentLevel: commitmentLevel,
			ExpectedBlock:   expectedBlock,
			ReceivedBlock:   blockNumber,
			GapSize:         gapSize,
			DetectedAt:      time.Now(),
			Topic:           topic,
		}

		// Still update state to track the new block
		tracker.LastBlock = blockNumber
		tracker.BlockCount++
		tracker.LastUpdated = time.Now()

		return gap
	}

	// Check for duplicate/old block (received block is behind or equal)
	if blockNumber <= tracker.LastBlock {
		// This is a duplicate or old block - acceptable in some scenarios
		// (e.g., redelivery, reorg correction)
		// Don't update state but don't raise gap either
		return nil
	}

	// Normal case: block is exactly as expected
	tracker.LastBlock = blockNumber
	tracker.BlockCount++
	tracker.LastUpdated = time.Now()

	return nil
}

// GetLastBlock returns the last seen block for a chain/commitment, or 0 if not tracked.
func (s *ChainState) GetLastBlock(chain protov1.Chain, commitmentLevel int16) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := chainStateKey{
		Chain:           chain,
		CommitmentLevel: commitmentLevel,
	}

	tracker, exists := s.chains[key]
	if !exists {
		return 0, false
	}

	return tracker.LastBlock, true
}

// GetState returns a snapshot of the current state for all chains.
func (s *ChainState) GetState() []ChainStateSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]ChainStateSnapshot, 0, len(s.chains))
	for key, tracker := range s.chains {
		// Copy tracker to avoid race
		trackerCopy := *tracker
		result = append(result, ChainStateSnapshot{
			Chain:           chainName(key.Chain),
			CommitmentLevel: key.CommitmentLevel,
			Tracker:         &trackerCopy,
		})
	}

	return result
}

// GetStats returns aggregated statistics.
func (s *ChainState) GetStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	totalBlocks := int64(0)
	totalGaps := int64(0)
	chainCount := len(s.chains)

	for _, tracker := range s.chains {
		totalBlocks += tracker.BlockCount
		totalGaps += tracker.GapCount
	}

	return map[string]interface{}{
		"chain_count":       chainCount,
		"total_blocks":      totalBlocks,
		"total_gaps":        totalGaps,
		"tracking_active":   chainCount > 0,
	}
}

// Reset clears all state. Used primarily for testing.
func (s *ChainState) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chains = make(map[chainStateKey]*BlockTracker)
}

// chainName converts a chain enum to a human-readable name.
func chainName(chain protov1.Chain) string {
	switch chain {
	case protov1.Chain_CHAIN_SOLANA:
		return "solana"
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
	default:
		return "unknown"
	}
}
