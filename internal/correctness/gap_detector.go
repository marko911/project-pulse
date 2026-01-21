package correctness

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

type GapEvent struct {
	Chain string

	CommitmentLevel protov1.CommitmentLevel

	ExpectedBlock uint64

	ReceivedBlock uint64

	GapSize uint64

	MissingBlocks []uint64

	DetectedAt time.Time
}

type GapCallback func(ctx context.Context, event *GapEvent) error

type ChainState struct {
	Chain             string
	CommitmentLevel   protov1.CommitmentLevel
	LastSeenBlock     uint64
	LastSeenHash      string
	LastSeenTimestamp time.Time
	GapsDetected      uint64
	EventsProcessed   uint64
}

type GapDetector struct {
	cfg    GapDetectorConfig
	logger *slog.Logger

	mu         sync.RWMutex
	chains     map[string]*ChainState
	callbacks  []GapCallback
	halted     bool
	haltReason string
}

type GapDetectorConfig struct {
	FailClosed bool

	MaxAllowedGap uint64

	AlertOnFirstBlock bool
}

func DefaultGapDetectorConfig() GapDetectorConfig {
	return GapDetectorConfig{
		FailClosed:        true,
		MaxAllowedGap:     1,
		AlertOnFirstBlock: false,
	}
}

func NewGapDetector(cfg GapDetectorConfig, logger *slog.Logger) *GapDetector {
	if logger == nil {
		logger = slog.Default()
	}

	return &GapDetector{
		cfg:    cfg,
		logger: logger.With("component", "gap-detector"),
		chains: make(map[string]*ChainState),
	}
}

func (d *GapDetector) OnGap(cb GapCallback) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.callbacks = append(d.callbacks, cb)
}

func chainKey(chain string, level protov1.CommitmentLevel) string {
	return fmt.Sprintf("%s:%d", chain, level)
}

func (d *GapDetector) ProcessEvent(ctx context.Context, event *protov1.CanonicalEvent) (*GapEvent, error) {
	if event == nil {
		return nil, fmt.Errorf("nil event")
	}

	if event.CommitmentLevel != protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED {
		return nil, nil
	}

	if event.ReorgAction != protov1.ReorgAction_REORG_ACTION_UNSPECIFIED &&
		event.ReorgAction != protov1.ReorgAction_REORG_ACTION_NORMAL {
		return nil, nil
	}

	chainName := chainNameFromProto(event.Chain)
	return d.processBlock(ctx, chainName, event.CommitmentLevel, event.BlockNumber, event.BlockHash)
}

func (d *GapDetector) processBlock(
	ctx context.Context,
	chain string,
	level protov1.CommitmentLevel,
	blockNum uint64,
	blockHash string,
) (*GapEvent, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.halted && d.cfg.FailClosed {
		return nil, fmt.Errorf("gap detector halted: %s", d.haltReason)
	}

	key := chainKey(chain, level)
	state := d.chains[key]

	if state == nil {
		d.logger.Info("initializing chain state",
			"chain", chain,
			"commitment_level", level,
			"first_block", blockNum,
		)
		d.chains[key] = &ChainState{
			Chain:             chain,
			CommitmentLevel:   level,
			LastSeenBlock:     blockNum,
			LastSeenHash:      blockHash,
			LastSeenTimestamp: time.Now(),
			EventsProcessed:   1,
		}
		return nil, nil
	}

	state.EventsProcessed++

	expectedBlock := state.LastSeenBlock + 1

	if blockNum <= state.LastSeenBlock {
		d.logger.Debug("received duplicate/old block",
			"chain", chain,
			"received", blockNum,
			"last_seen", state.LastSeenBlock,
		)
		return nil, nil
	}

	if blockNum == expectedBlock {
		state.LastSeenBlock = blockNum
		state.LastSeenHash = blockHash
		state.LastSeenTimestamp = time.Now()
		return nil, nil
	}

	gapSize := blockNum - expectedBlock
	d.logger.Error("GAP DETECTED",
		"chain", chain,
		"commitment_level", level,
		"expected", expectedBlock,
		"received", blockNum,
		"gap_size", gapSize,
	)

	state.GapsDetected++

	gapEvent := &GapEvent{
		Chain:           chain,
		CommitmentLevel: level,
		ExpectedBlock:   expectedBlock,
		ReceivedBlock:   blockNum,
		GapSize:         gapSize,
		MissingBlocks:   make([]uint64, 0, gapSize),
		DetectedAt:      time.Now(),
	}

	for b := expectedBlock; b < blockNum; b++ {
		gapEvent.MissingBlocks = append(gapEvent.MissingBlocks, b)
	}

	for _, cb := range d.callbacks {
		if err := cb(ctx, gapEvent); err != nil {
			d.logger.Error("gap callback failed", "error", err)
		}
	}

	if d.cfg.FailClosed {
		d.halted = true
		d.haltReason = fmt.Sprintf("gap detected: chain=%s, missing blocks %d-%d",
			chain, expectedBlock, blockNum-1)

		state.LastSeenBlock = blockNum
		state.LastSeenHash = blockHash
		state.LastSeenTimestamp = time.Now()

		return gapEvent, fmt.Errorf("fail-closed: %s", d.haltReason)
	}

	state.LastSeenBlock = blockNum
	state.LastSeenHash = blockHash
	state.LastSeenTimestamp = time.Now()

	return gapEvent, nil
}

func (d *GapDetector) ResolveGap(chain string, level protov1.CommitmentLevel, upToBlock uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	key := chainKey(chain, level)
	state := d.chains[key]
	if state == nil {
		return fmt.Errorf("unknown chain state: %s", key)
	}

	d.logger.Info("gap resolved",
		"chain", chain,
		"commitment_level", level,
		"up_to_block", upToBlock,
	)

	if d.halted {
		d.halted = false
		d.haltReason = ""
		d.logger.Info("gap detector resumed after resolution")
	}

	return nil
}

func (d *GapDetector) IsHalted() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.halted
}

func (d *GapDetector) HaltReason() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.haltReason
}

func (d *GapDetector) GetChainState(chain string, level protov1.CommitmentLevel) *ChainState {
	d.mu.RLock()
	defer d.mu.RUnlock()
	key := chainKey(chain, level)
	if state := d.chains[key]; state != nil {
		copy := *state
		return &copy
	}
	return nil
}

func (d *GapDetector) Stats() map[string]interface{} {
	d.mu.RLock()
	defer d.mu.RUnlock()

	chainStats := make(map[string]interface{})
	for key, state := range d.chains {
		chainStats[key] = map[string]interface{}{
			"last_seen_block":   state.LastSeenBlock,
			"last_seen_hash":    truncateHash(state.LastSeenHash),
			"gaps_detected":     state.GapsDetected,
			"events_processed":  state.EventsProcessed,
			"last_seen_at":      state.LastSeenTimestamp,
		}
	}

	return map[string]interface{}{
		"chains": chainStats,
		"halted": d.halted,
		"reason": d.haltReason,
	}
}

func chainNameFromProto(chain protov1.Chain) string {
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

func truncateHash(hash string) string {
	if len(hash) > 16 {
		return hash[:16]
	}
	return hash
}
