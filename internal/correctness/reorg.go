// Package correctness implements chain verification and reorg detection.
package correctness

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/mirador/pulse/internal/adapter"
	protov1 "github.com/mirador/pulse/pkg/proto/v1"
)

// ReorgEvent represents a detected chain reorganization.
type ReorgEvent struct {
	// Chain identifier
	Chain string

	// ForkPoint is the last common block before the fork
	ForkPoint uint64

	// OrphanedBlocks are the block numbers that were orphaned
	OrphanedBlocks []uint64

	// OrphanedHashes are the block hashes of orphaned blocks
	OrphanedHashes []string

	// NewBlocks are the block numbers in the new canonical chain
	NewBlocks []uint64

	// NewHashes are the block hashes of the new canonical blocks
	NewHashes []string

	// Depth is how many blocks were reorganized
	Depth int
}

// ReorgCallback is called when a reorg is detected.
type ReorgCallback func(ctx context.Context, event *ReorgEvent) error

// BlockInfo holds minimal block information for reorg detection.
type BlockInfo struct {
	Number     uint64
	Hash       string
	ParentHash string
	Timestamp  int64
}

// ReorgDetector detects blockchain reorganizations by tracking block parent hashes.
type ReorgDetector struct {
	cfg    ReorgDetectorConfig
	logger *slog.Logger

	mu           sync.RWMutex
	chainHeads   map[string]*BlockInfo            // chain -> current head
	blockIndex   map[string]map[string]*BlockInfo // chain -> hash -> block info
	heightIndex  map[string]map[uint64]*BlockInfo // chain -> height -> block info
	maxTracked   int                              // Max blocks to keep in memory per chain

	callbacks []ReorgCallback
}

// ReorgDetectorConfig configures the reorg detector.
type ReorgDetectorConfig struct {
	// MaxTrackedBlocks is the maximum number of blocks to keep in memory per chain.
	// Older blocks are pruned to prevent unbounded memory growth.
	// Default: 1000
	MaxTrackedBlocks int

	// MinConfirmations is the minimum number of confirmations before considering
	// a block stable (used for pruning older blocks).
	// Default: 12
	MinConfirmations int
}

// DefaultReorgDetectorConfig returns sensible defaults.
func DefaultReorgDetectorConfig() ReorgDetectorConfig {
	return ReorgDetectorConfig{
		MaxTrackedBlocks: 1000,
		MinConfirmations: 12,
	}
}

// NewReorgDetector creates a new reorg detector.
func NewReorgDetector(cfg ReorgDetectorConfig, logger *slog.Logger) *ReorgDetector {
	if cfg.MaxTrackedBlocks <= 0 {
		cfg.MaxTrackedBlocks = 1000
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &ReorgDetector{
		cfg:         cfg,
		logger:      logger.With("component", "reorg-detector"),
		chainHeads:  make(map[string]*BlockInfo),
		blockIndex:  make(map[string]map[string]*BlockInfo),
		heightIndex: make(map[string]map[uint64]*BlockInfo),
		maxTracked:  cfg.MaxTrackedBlocks,
	}
}

// OnReorg registers a callback for reorg events.
func (d *ReorgDetector) OnReorg(cb ReorgCallback) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.callbacks = append(d.callbacks, cb)
}

// ProcessBlock processes a new block and detects any reorganization.
// Returns a ReorgEvent if a reorg was detected, nil otherwise.
func (d *ReorgDetector) ProcessBlock(ctx context.Context, block *BlockInfo) (*ReorgEvent, error) {
	if block == nil {
		return nil, fmt.Errorf("nil block")
	}

	// Use "default" chain if no chain specified
	return d.ProcessBlockForChain(ctx, "default", block)
}

// ProcessBlockForChain processes a block for a specific chain.
func (d *ReorgDetector) ProcessBlockForChain(ctx context.Context, chain string, block *BlockInfo) (*ReorgEvent, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Initialize chain indices if needed
	if d.blockIndex[chain] == nil {
		d.blockIndex[chain] = make(map[string]*BlockInfo)
	}
	if d.heightIndex[chain] == nil {
		d.heightIndex[chain] = make(map[uint64]*BlockInfo)
	}

	currentHead := d.chainHeads[chain]

	// First block for this chain
	if currentHead == nil {
		d.logger.Info("first block for chain",
			"chain", chain,
			"block", block.Number,
			"hash", block.Hash[:16],
		)
		d.updateChainState(chain, block)
		return nil, nil
	}

	// Check if this extends the current chain
	if block.ParentHash == currentHead.Hash {
		// Normal case: block extends the chain
		d.logger.Debug("block extends chain",
			"chain", chain,
			"block", block.Number,
			"parent", block.ParentHash[:16],
		)
		d.updateChainState(chain, block)
		return nil, nil
	}

	// Check if we've seen this block before (duplicate)
	if existing := d.blockIndex[chain][block.Hash]; existing != nil {
		d.logger.Debug("duplicate block received",
			"chain", chain,
			"block", block.Number,
			"hash", block.Hash[:16],
		)
		return nil, nil
	}

	// Parent hash mismatch - potential reorg
	d.logger.Warn("parent hash mismatch - potential reorg",
		"chain", chain,
		"block", block.Number,
		"expected_parent", currentHead.Hash[:16],
		"actual_parent", block.ParentHash[:16],
	)

	// Find the fork point
	reorgEvent := d.detectReorg(chain, block)
	if reorgEvent != nil {
		d.logger.Warn("reorg detected",
			"chain", chain,
			"fork_point", reorgEvent.ForkPoint,
			"depth", reorgEvent.Depth,
			"orphaned", len(reorgEvent.OrphanedBlocks),
		)

		// Update chain state to the new head
		d.updateChainState(chain, block)

		// Notify callbacks
		for _, cb := range d.callbacks {
			if err := cb(ctx, reorgEvent); err != nil {
				d.logger.Error("reorg callback failed", "error", err)
			}
		}

		return reorgEvent, nil
	}

	// Could be a late-arriving block from a side chain
	d.logger.Debug("late block from side chain",
		"chain", chain,
		"block", block.Number,
	)
	d.indexBlock(chain, block)

	return nil, nil
}

// detectReorg finds the fork point and constructs a ReorgEvent.
func (d *ReorgDetector) detectReorg(chain string, newBlock *BlockInfo) *ReorgEvent {
	// Walk back from new block to find where it connects to our chain
	parentHash := newBlock.ParentHash
	var newChain []*BlockInfo
	newChain = append(newChain, newBlock)

	// Find the connection point by looking up the fork block's parent in our chain index
	parentBlock := d.blockIndex[chain][parentHash]
	if parentBlock == nil {
		// Can't find parent - need to fetch more history
		if len(parentHash) >= 16 {
			d.logger.Debug("parent not found in index",
				"chain", chain,
				"parent", parentHash[:16],
			)
		}
		return nil
	}

	// Found the fork point - the parent block is where both chains connect
	forkPoint := parentBlock.Number

	// Find orphaned blocks (blocks after fork point on old chain)
	currentHead := d.chainHeads[chain]
	var orphaned []*BlockInfo
	block := currentHead
	for block != nil && block.Number > forkPoint {
		orphaned = append(orphaned, block)
		block = d.blockIndex[chain][block.ParentHash]
	}

	// Build reorg event
	event := &ReorgEvent{
		Chain:     chain,
		ForkPoint: forkPoint,
		Depth:     len(orphaned),
	}

	for _, b := range orphaned {
		event.OrphanedBlocks = append(event.OrphanedBlocks, b.Number)
		event.OrphanedHashes = append(event.OrphanedHashes, b.Hash)
	}

	for _, b := range newChain {
		event.NewBlocks = append(event.NewBlocks, b.Number)
		event.NewHashes = append(event.NewHashes, b.Hash)
	}

	return event
}

// updateChainState updates the chain head and indices.
func (d *ReorgDetector) updateChainState(chain string, block *BlockInfo) {
	d.chainHeads[chain] = block
	d.indexBlock(chain, block)
	d.pruneOldBlocks(chain)
}

// indexBlock adds a block to the indices.
func (d *ReorgDetector) indexBlock(chain string, block *BlockInfo) {
	d.blockIndex[chain][block.Hash] = block
	d.heightIndex[chain][block.Number] = block
}

// pruneOldBlocks removes old blocks to prevent unbounded memory growth.
func (d *ReorgDetector) pruneOldBlocks(chain string) {
	head := d.chainHeads[chain]
	if head == nil {
		return
	}

	// Avoid uint64 underflow: only prune if we have more blocks than maxTracked
	if head.Number <= uint64(d.maxTracked) {
		return
	}
	cutoff := head.Number - uint64(d.maxTracked)

	// Remove blocks below cutoff
	for hash, block := range d.blockIndex[chain] {
		if block.Number < cutoff {
			delete(d.blockIndex[chain], hash)
			delete(d.heightIndex[chain], block.Number)
		}
	}
}

// GetChainHead returns the current head block for a chain.
func (d *ReorgDetector) GetChainHead(chain string) *BlockInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.chainHeads[chain]
}

// GetBlockByHash returns a block by its hash.
func (d *ReorgDetector) GetBlockByHash(chain, hash string) *BlockInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.blockIndex[chain] == nil {
		return nil
	}
	return d.blockIndex[chain][hash]
}

// GetBlockByHeight returns a block by its height.
func (d *ReorgDetector) GetBlockByHeight(chain string, height uint64) *BlockInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.heightIndex[chain] == nil {
		return nil
	}
	return d.heightIndex[chain][height]
}

// ProcessAdapterEvent extracts block info from an adapter event and processes it.
func (d *ReorgDetector) ProcessAdapterEvent(ctx context.Context, event adapter.Event) (*ReorgEvent, error) {
	// Only process block-level events
	if event.EventType != "block" && event.BlockHash == "" {
		return nil, nil
	}

	block := &BlockInfo{
		Number:     event.BlockNumber,
		Hash:       event.BlockHash,
		ParentHash: event.ParentHash,
		Timestamp:  event.Timestamp,
	}

	return d.ProcessBlockForChain(ctx, event.Chain, block)
}

// CreateRetractionEvents generates retraction events for orphaned blocks.
func (d *ReorgDetector) CreateRetractionEvents(reorg *ReorgEvent, originalEvents []*protov1.CanonicalEvent) []*protov1.CanonicalEvent {
	var retractions []*protov1.CanonicalEvent

	// Create a set of orphaned block numbers for fast lookup
	orphanedSet := make(map[uint64]bool)
	for _, num := range reorg.OrphanedBlocks {
		orphanedSet[num] = true
	}

	// Create retraction events for events in orphaned blocks
	for _, event := range originalEvents {
		if orphanedSet[event.BlockNumber] {
			retraction := &protov1.CanonicalEvent{
				EventId:         event.EventId + ":retract",
				Chain:           event.Chain,
				CommitmentLevel: event.CommitmentLevel,
				BlockNumber:     event.BlockNumber,
				BlockHash:       event.BlockHash,
				TxHash:          event.TxHash,
				TxIndex:         event.TxIndex,
				EventIndex:      event.EventIndex,
				EventType:       event.EventType,
				Accounts:        event.Accounts,
				Timestamp:       event.Timestamp,
				Payload:         event.Payload,
				ReorgAction:     protov1.ReorgAction_REORG_ACTION_RETRACT,
				SchemaVersion:   event.SchemaVersion,
				ProgramId:       event.ProgramId,
			}
			retractions = append(retractions, retraction)
		}
	}

	return retractions
}

// CreateReplacementEvents generates replacement events for new canonical blocks.
// The replacementEvents are the events from the new canonical chain that replace
// the retracted events. This links new events to their retracted counterparts.
func (d *ReorgDetector) CreateReplacementEvents(reorg *ReorgEvent, retractedEvents []*protov1.CanonicalEvent, newEvents []*protov1.CanonicalEvent) []*protov1.CanonicalEvent {
	var replacements []*protov1.CanonicalEvent

	// Build a map of retracted events by (block_number, tx_index, event_index)
	// to find corresponding replacements
	type eventKey struct {
		blockNum   uint64
		txIndex    uint32
		eventIndex uint32
	}
	retractedMap := make(map[eventKey]*protov1.CanonicalEvent)
	for _, event := range retractedEvents {
		key := eventKey{
			blockNum:   event.BlockNumber,
			txIndex:    event.TxIndex,
			eventIndex: event.EventIndex,
		}
		retractedMap[key] = event
	}

	// Create a set of new block numbers for fast lookup
	newBlockSet := make(map[uint64]bool)
	for _, num := range reorg.NewBlocks {
		newBlockSet[num] = true
	}

	// For each new event in the new canonical chain
	for _, event := range newEvents {
		if !newBlockSet[event.BlockNumber] {
			continue
		}

		key := eventKey{
			blockNum:   event.BlockNumber,
			txIndex:    event.TxIndex,
			eventIndex: event.EventIndex,
		}

		replacement := &protov1.CanonicalEvent{
			EventId:         event.EventId + ":replace",
			Chain:           event.Chain,
			CommitmentLevel: event.CommitmentLevel,
			BlockNumber:     event.BlockNumber,
			BlockHash:       event.BlockHash,
			TxHash:          event.TxHash,
			TxIndex:         event.TxIndex,
			EventIndex:      event.EventIndex,
			EventType:       event.EventType,
			Accounts:        event.Accounts,
			Timestamp:       event.Timestamp,
			Payload:         event.Payload,
			ReorgAction:     protov1.ReorgAction_REORG_ACTION_REPLACE,
			SchemaVersion:   event.SchemaVersion,
			ProgramId:       event.ProgramId,
			NativeValue:     event.NativeValue,
		}

		// Link to the retracted event if one exists at the same position
		if retracted := retractedMap[key]; retracted != nil {
			replacement.ReplacesEventId = retracted.EventId
		}

		replacements = append(replacements, replacement)
	}

	return replacements
}

// EmitReorgEvents generates both retraction and replacement events for a reorg.
// Returns (retractions, replacements) slices.
func (d *ReorgDetector) EmitReorgEvents(reorg *ReorgEvent, orphanedEvents []*protov1.CanonicalEvent, newCanonicalEvents []*protov1.CanonicalEvent) ([]*protov1.CanonicalEvent, []*protov1.CanonicalEvent) {
	retractions := d.CreateRetractionEvents(reorg, orphanedEvents)
	replacements := d.CreateReplacementEvents(reorg, retractions, newCanonicalEvents)
	return retractions, replacements
}

// Stats returns current detector statistics.
func (d *ReorgDetector) Stats() map[string]interface{} {
	d.mu.RLock()
	defer d.mu.RUnlock()

	chainStats := make(map[string]interface{})
	for chain, head := range d.chainHeads {
		chainStats[chain] = map[string]interface{}{
			"head_number":   head.Number,
			"head_hash":     head.Hash[:16],
			"tracked_blocks": len(d.blockIndex[chain]),
		}
	}

	return map[string]interface{}{
		"chains": chainStats,
	}
}
