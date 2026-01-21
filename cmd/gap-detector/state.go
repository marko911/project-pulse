package main

import (
	"sync"
	"time"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

type ChainState struct {
	mu     sync.RWMutex
	chains map[chainStateKey]*BlockTracker
}

type chainStateKey struct {
	Chain           protov1.Chain
	CommitmentLevel int16
}

type BlockTracker struct {
	LastBlock   uint64    `json:"last_block"`
	FirstBlock  uint64    `json:"first_block"`
	BlockCount  int64     `json:"block_count"`
	GapCount    int64     `json:"gap_count"`
	LastUpdated time.Time `json:"last_updated"`
	Initialized bool      `json:"initialized"`
}

type ChainStateSnapshot struct {
	Chain           string        `json:"chain"`
	CommitmentLevel int16         `json:"commitment_level"`
	Tracker         *BlockTracker `json:"tracker"`
}

func NewChainState() *ChainState {
	return &ChainState{
		chains: make(map[chainStateKey]*BlockTracker),
	}
}

func (s *ChainState) CheckAndUpdate(chain protov1.Chain, commitmentLevel int16, blockNumber uint64, topic string) *GapEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := chainStateKey{
		Chain:           chain,
		CommitmentLevel: commitmentLevel,
	}

	tracker, exists := s.chains[key]
	if !exists {
		s.chains[key] = &BlockTracker{
			LastBlock:   blockNumber,
			FirstBlock:  blockNumber,
			BlockCount:  1,
			LastUpdated: time.Now(),
			Initialized: true,
		}
		return nil
	}

	expectedBlock := tracker.LastBlock + 1

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

		tracker.LastBlock = blockNumber
		tracker.BlockCount++
		tracker.LastUpdated = time.Now()

		return gap
	}

	if blockNumber <= tracker.LastBlock {
		return nil
	}

	tracker.LastBlock = blockNumber
	tracker.BlockCount++
	tracker.LastUpdated = time.Now()

	return nil
}

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

func (s *ChainState) GetState() []ChainStateSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]ChainStateSnapshot, 0, len(s.chains))
	for key, tracker := range s.chains {
		trackerCopy := *tracker
		result = append(result, ChainStateSnapshot{
			Chain:           chainName(key.Chain),
			CommitmentLevel: key.CommitmentLevel,
			Tracker:         &trackerCopy,
		})
	}

	return result
}

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

func (s *ChainState) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chains = make(map[chainStateKey]*BlockTracker)
}

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
