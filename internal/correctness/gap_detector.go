// Package correctness implements chain verification and reorg detection.
package correctness

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

// GapEvent represents a detected gap in the block sequence.
type GapEvent struct {
	// Chain identifier (e.g., "ethereum", "solana")
	Chain string

	// CommitmentLevel at which the gap was detected
	CommitmentLevel protov1.CommitmentLevel

	// ExpectedBlock is the block number we expected to see next
	ExpectedBlock uint64

	// ReceivedBlock is the block number we actually received
	ReceivedBlock uint64

	// GapSize is the number of missing blocks
	GapSize uint64

	// MissingBlocks lists all block numbers in the gap
	MissingBlocks []uint64

	// DetectedAt is when the gap was detected
	DetectedAt time.Time
}

// GapCallback is called when a gap is detected.
type GapCallback func(ctx context.Context, event *GapEvent) error

// ChainState tracks the block progression state for a single chain.
type ChainState struct {
	Chain             string
	CommitmentLevel   protov1.CommitmentLevel
	LastSeenBlock     uint64
	LastSeenHash      string
	LastSeenTimestamp time.Time
	GapsDetected      uint64
	EventsProcessed   uint64
}

// GapDetector monitors finalized block sequences and detects missing blocks.
// It implements fail-closed behavior: detection of gaps halts processing until
// resolved (via backfill or manual intervention).
type GapDetector struct {
	cfg    GapDetectorConfig
	logger *slog.Logger

	mu         sync.RWMutex
	chains     map[string]*ChainState // key: "chain:commitment_level"
	callbacks  []GapCallback
	halted     bool
	haltReason string
}

// GapDetectorConfig configures the gap detector behavior.
type GapDetectorConfig struct {
	// FailClosed determines whether to halt on gap detection.
	// If true, ProcessEvent returns an error when a gap is detected,
	// preventing further processing until the gap is resolved.
	// Default: true (Milestone 3 requirement)
	FailClosed bool

	// MaxAllowedGap is the maximum gap size before triggering an alert.
	// Gaps larger than this are considered critical.
	// Default: 1 (any gap triggers alert)
	MaxAllowedGap uint64

	// AlertOnFirstBlock determines whether to alert when seeing
	// a chain for the first time (no prior state).
	// Default: false
	AlertOnFirstBlock bool
}

// DefaultGapDetectorConfig returns sensible defaults for production.
func DefaultGapDetectorConfig() GapDetectorConfig {
	return GapDetectorConfig{
		FailClosed:        true,  // Milestone 3: fail-closed
		MaxAllowedGap:     1,     // Any gap triggers
		AlertOnFirstBlock: false, // Don't alert on bootstrap
	}
}

// NewGapDetector creates a new gap detector.
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

// OnGap registers a callback for gap events.
func (d *GapDetector) OnGap(cb GapCallback) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.callbacks = append(d.callbacks, cb)
}

// chainKey generates a unique key for chain + commitment level.
func chainKey(chain string, level protov1.CommitmentLevel) string {
	return fmt.Sprintf("%s:%d", chain, level)
}

// ProcessEvent processes an incoming event and checks for gaps.
// Returns a GapEvent if a gap was detected, nil otherwise.
// If FailClosed is enabled and a gap is detected, returns an error.
func (d *GapDetector) ProcessEvent(ctx context.Context, event *protov1.CanonicalEvent) (*GapEvent, error) {
	if event == nil {
		return nil, fmt.Errorf("nil event")
	}

	// Skip non-finalized events for gap detection
	if event.CommitmentLevel != protov1.CommitmentLevel_COMMITMENT_LEVEL_FINALIZED {
		return nil, nil
	}

	// Skip reorg-related events (retractions/replacements handled separately)
	if event.ReorgAction != protov1.ReorgAction_REORG_ACTION_UNSPECIFIED &&
		event.ReorgAction != protov1.ReorgAction_REORG_ACTION_NORMAL {
		return nil, nil
	}

	chainName := chainNameFromProto(event.Chain)
	return d.processBlock(ctx, chainName, event.CommitmentLevel, event.BlockNumber, event.BlockHash)
}

// processBlock checks if the block number is sequential.
func (d *GapDetector) processBlock(
	ctx context.Context,
	chain string,
	level protov1.CommitmentLevel,
	blockNum uint64,
	blockHash string,
) (*GapEvent, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Check if halted
	if d.halted && d.cfg.FailClosed {
		return nil, fmt.Errorf("gap detector halted: %s", d.haltReason)
	}

	key := chainKey(chain, level)
	state := d.chains[key]

	// First block for this chain/level
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

	// Check for gap
	expectedBlock := state.LastSeenBlock + 1

	// Duplicate or already-processed block
	if blockNum <= state.LastSeenBlock {
		d.logger.Debug("received duplicate/old block",
			"chain", chain,
			"received", blockNum,
			"last_seen", state.LastSeenBlock,
		)
		return nil, nil
	}

	// Sequential - no gap
	if blockNum == expectedBlock {
		state.LastSeenBlock = blockNum
		state.LastSeenHash = blockHash
		state.LastSeenTimestamp = time.Now()
		return nil, nil
	}

	// Gap detected!
	gapSize := blockNum - expectedBlock
	d.logger.Error("GAP DETECTED",
		"chain", chain,
		"commitment_level", level,
		"expected", expectedBlock,
		"received", blockNum,
		"gap_size", gapSize,
	)

	state.GapsDetected++

	// Build gap event
	gapEvent := &GapEvent{
		Chain:           chain,
		CommitmentLevel: level,
		ExpectedBlock:   expectedBlock,
		ReceivedBlock:   blockNum,
		GapSize:         gapSize,
		MissingBlocks:   make([]uint64, 0, gapSize),
		DetectedAt:      time.Now(),
	}

	// Enumerate missing blocks
	for b := expectedBlock; b < blockNum; b++ {
		gapEvent.MissingBlocks = append(gapEvent.MissingBlocks, b)
	}

	// Notify callbacks
	for _, cb := range d.callbacks {
		if err := cb(ctx, gapEvent); err != nil {
			d.logger.Error("gap callback failed", "error", err)
		}
	}

	// Fail-closed behavior
	if d.cfg.FailClosed {
		d.halted = true
		d.haltReason = fmt.Sprintf("gap detected: chain=%s, missing blocks %d-%d",
			chain, expectedBlock, blockNum-1)

		// Update state to the new block anyway (for recovery)
		state.LastSeenBlock = blockNum
		state.LastSeenHash = blockHash
		state.LastSeenTimestamp = time.Now()

		return gapEvent, fmt.Errorf("fail-closed: %s", d.haltReason)
	}

	// Non-fail-closed: update state and continue
	state.LastSeenBlock = blockNum
	state.LastSeenHash = blockHash
	state.LastSeenTimestamp = time.Now()

	return gapEvent, nil
}

// ResolveGap marks a gap as resolved, allowing processing to continue.
// This should be called after backfill has completed.
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

	// If this resolution covers our halt point, clear the halt
	if d.halted {
		d.halted = false
		d.haltReason = ""
		d.logger.Info("gap detector resumed after resolution")
	}

	return nil
}

// IsHalted returns whether the detector is halted due to a gap.
func (d *GapDetector) IsHalted() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.halted
}

// HaltReason returns the reason for the halt, if halted.
func (d *GapDetector) HaltReason() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.haltReason
}

// GetChainState returns the current state for a chain/commitment level.
func (d *GapDetector) GetChainState(chain string, level protov1.CommitmentLevel) *ChainState {
	d.mu.RLock()
	defer d.mu.RUnlock()
	key := chainKey(chain, level)
	if state := d.chains[key]; state != nil {
		// Return a copy to avoid race conditions
		copy := *state
		return &copy
	}
	return nil
}

// Stats returns current detector statistics.
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

// chainNameFromProto converts a protobuf Chain enum to a string.
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

// truncateHash safely truncates a hash for logging.
func truncateHash(hash string) string {
	if len(hash) > 16 {
		return hash[:16]
	}
	return hash
}
