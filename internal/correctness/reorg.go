package correctness

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/marko911/project-pulse/internal/adapter"
	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

type ReorgEvent struct {
	Chain string

	ForkPoint uint64

	OrphanedBlocks []uint64

	OrphanedHashes []string

	NewBlocks []uint64

	NewHashes []string

	Depth int
}

type ReorgCallback func(ctx context.Context, event *ReorgEvent) error

type BlockInfo struct {
	Number     uint64
	Hash       string
	ParentHash string
	Timestamp  int64
}

type ReorgDetector struct {
	cfg    ReorgDetectorConfig
	logger *slog.Logger

	mu           sync.RWMutex
	chainHeads   map[string]*BlockInfo
	blockIndex   map[string]map[string]*BlockInfo
	heightIndex  map[string]map[uint64]*BlockInfo
	maxTracked   int

	callbacks []ReorgCallback
}

type ReorgDetectorConfig struct {
	MaxTrackedBlocks int

	MinConfirmations int
}

func DefaultReorgDetectorConfig() ReorgDetectorConfig {
	return ReorgDetectorConfig{
		MaxTrackedBlocks: 1000,
		MinConfirmations: 12,
	}
}

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

func (d *ReorgDetector) OnReorg(cb ReorgCallback) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.callbacks = append(d.callbacks, cb)
}

func (d *ReorgDetector) ProcessBlock(ctx context.Context, block *BlockInfo) (*ReorgEvent, error) {
	if block == nil {
		return nil, fmt.Errorf("nil block")
	}

	return d.ProcessBlockForChain(ctx, "default", block)
}

func (d *ReorgDetector) ProcessBlockForChain(ctx context.Context, chain string, block *BlockInfo) (*ReorgEvent, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.blockIndex[chain] == nil {
		d.blockIndex[chain] = make(map[string]*BlockInfo)
	}
	if d.heightIndex[chain] == nil {
		d.heightIndex[chain] = make(map[uint64]*BlockInfo)
	}

	currentHead := d.chainHeads[chain]

	if currentHead == nil {
		d.logger.Info("first block for chain",
			"chain", chain,
			"block", block.Number,
			"hash", block.Hash[:16],
		)
		d.updateChainState(chain, block)
		return nil, nil
	}

	if block.ParentHash == currentHead.Hash {
		d.logger.Debug("block extends chain",
			"chain", chain,
			"block", block.Number,
			"parent", block.ParentHash[:16],
		)
		d.updateChainState(chain, block)
		return nil, nil
	}

	if existing := d.blockIndex[chain][block.Hash]; existing != nil {
		d.logger.Debug("duplicate block received",
			"chain", chain,
			"block", block.Number,
			"hash", block.Hash[:16],
		)
		return nil, nil
	}

	d.logger.Warn("parent hash mismatch - potential reorg",
		"chain", chain,
		"block", block.Number,
		"expected_parent", currentHead.Hash[:16],
		"actual_parent", block.ParentHash[:16],
	)

	reorgEvent := d.detectReorg(chain, block)
	if reorgEvent != nil {
		d.logger.Warn("reorg detected",
			"chain", chain,
			"fork_point", reorgEvent.ForkPoint,
			"depth", reorgEvent.Depth,
			"orphaned", len(reorgEvent.OrphanedBlocks),
		)

		d.updateChainState(chain, block)

		for _, cb := range d.callbacks {
			if err := cb(ctx, reorgEvent); err != nil {
				d.logger.Error("reorg callback failed", "error", err)
			}
		}

		return reorgEvent, nil
	}

	d.logger.Debug("late block from side chain",
		"chain", chain,
		"block", block.Number,
	)
	d.indexBlock(chain, block)

	return nil, nil
}

func (d *ReorgDetector) detectReorg(chain string, newBlock *BlockInfo) *ReorgEvent {
	parentHash := newBlock.ParentHash
	var newChain []*BlockInfo
	newChain = append(newChain, newBlock)

	parentBlock := d.blockIndex[chain][parentHash]
	if parentBlock == nil {
		if len(parentHash) >= 16 {
			d.logger.Debug("parent not found in index",
				"chain", chain,
				"parent", parentHash[:16],
			)
		}
		return nil
	}

	forkPoint := parentBlock.Number

	currentHead := d.chainHeads[chain]
	var orphaned []*BlockInfo
	block := currentHead
	for block != nil && block.Number > forkPoint {
		orphaned = append(orphaned, block)
		block = d.blockIndex[chain][block.ParentHash]
	}

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

func (d *ReorgDetector) updateChainState(chain string, block *BlockInfo) {
	d.chainHeads[chain] = block
	d.indexBlock(chain, block)
	d.pruneOldBlocks(chain)
}

func (d *ReorgDetector) indexBlock(chain string, block *BlockInfo) {
	d.blockIndex[chain][block.Hash] = block
	d.heightIndex[chain][block.Number] = block
}

func (d *ReorgDetector) pruneOldBlocks(chain string) {
	head := d.chainHeads[chain]
	if head == nil {
		return
	}

	if head.Number <= uint64(d.maxTracked) {
		return
	}
	cutoff := head.Number - uint64(d.maxTracked)

	for hash, block := range d.blockIndex[chain] {
		if block.Number < cutoff {
			delete(d.blockIndex[chain], hash)
			delete(d.heightIndex[chain], block.Number)
		}
	}
}

func (d *ReorgDetector) GetChainHead(chain string) *BlockInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.chainHeads[chain]
}

func (d *ReorgDetector) GetBlockByHash(chain, hash string) *BlockInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.blockIndex[chain] == nil {
		return nil
	}
	return d.blockIndex[chain][hash]
}

func (d *ReorgDetector) GetBlockByHeight(chain string, height uint64) *BlockInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.heightIndex[chain] == nil {
		return nil
	}
	return d.heightIndex[chain][height]
}

func (d *ReorgDetector) ProcessAdapterEvent(ctx context.Context, event adapter.Event) (*ReorgEvent, error) {
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

func (d *ReorgDetector) CreateRetractionEvents(reorg *ReorgEvent, originalEvents []*protov1.CanonicalEvent) []*protov1.CanonicalEvent {
	var retractions []*protov1.CanonicalEvent

	orphanedSet := make(map[uint64]bool)
	for _, num := range reorg.OrphanedBlocks {
		orphanedSet[num] = true
	}

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

func (d *ReorgDetector) CreateReplacementEvents(reorg *ReorgEvent, retractedEvents []*protov1.CanonicalEvent, newEvents []*protov1.CanonicalEvent) []*protov1.CanonicalEvent {
	var replacements []*protov1.CanonicalEvent

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

	newBlockSet := make(map[uint64]bool)
	for _, num := range reorg.NewBlocks {
		newBlockSet[num] = true
	}

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

		if retracted := retractedMap[key]; retracted != nil {
			replacement.ReplacesEventId = retracted.EventId
		}

		replacements = append(replacements, replacement)
	}

	return replacements
}

func (d *ReorgDetector) EmitReorgEvents(reorg *ReorgEvent, orphanedEvents []*protov1.CanonicalEvent, newCanonicalEvents []*protov1.CanonicalEvent) ([]*protov1.CanonicalEvent, []*protov1.CanonicalEvent) {
	retractions := d.CreateRetractionEvents(reorg, orphanedEvents)
	replacements := d.CreateReplacementEvents(reorg, retractions, newCanonicalEvents)
	return retractions, replacements
}

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
