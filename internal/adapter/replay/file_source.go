package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/marko911/project-pulse/internal/adapter"
)

type Fixture struct {
	Chain       string          `json:"chain"`
	Type        string          `json:"type"`
	RecordedAt  time.Time       `json:"recorded_at"`
	BlockNumber uint64          `json:"block_number,omitempty"`
	BlockHash   string          `json:"block_hash,omitempty"`
	Data        json.RawMessage `json:"data"`
}

type EVMBlockFixture struct {
	Number       uint64   `json:"number"`
	Hash         string   `json:"hash"`
	ParentHash   string   `json:"parent_hash"`
	Timestamp    uint64   `json:"timestamp"`
	GasLimit     uint64   `json:"gas_limit"`
	GasUsed      uint64   `json:"gas_used"`
	BaseFee      string   `json:"base_fee,omitempty"`
	Transactions []string `json:"transactions"`
}

type EVMLogFixture struct {
	Address     string   `json:"address"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
	BlockNumber uint64   `json:"block_number"`
	TxHash      string   `json:"tx_hash"`
	TxIndex     uint     `json:"tx_index"`
	BlockHash   string   `json:"block_hash"`
	LogIndex    uint     `json:"log_index"`
	Removed     bool     `json:"removed"`
}

type SolanaBlockFixture struct {
	Slot              uint64   `json:"slot"`
	Blockhash         string   `json:"blockhash"`
	PreviousBlockhash string   `json:"previous_blockhash"`
	ParentSlot        uint64   `json:"parent_slot"`
	BlockTime         int64    `json:"block_time,omitempty"`
	BlockHeight       uint64   `json:"block_height,omitempty"`
	Transactions      []string `json:"transactions"`
}

type SolanaTransactionFixture struct {
	Signature   string   `json:"signature"`
	Slot        uint64   `json:"slot"`
	BlockTime   int64    `json:"block_time,omitempty"`
	Err         string   `json:"err,omitempty"`
	Fee         uint64   `json:"fee"`
	Accounts    []string `json:"accounts"`
	ProgramIDs  []string `json:"program_ids"`
	LogMessages []string `json:"log_messages,omitempty"`
}

type FileSourceConfig struct {
	Chain string

	FixturesDir string

	Loop bool

	PlaybackSpeed float64
}

type FileSource struct {
	cfg    FileSourceConfig
	logger *slog.Logger
}

func NewFileSource(cfg FileSourceConfig, logger *slog.Logger) *FileSource {
	if logger == nil {
		logger = slog.Default()
	}
	return &FileSource{
		cfg:    cfg,
		logger: logger.With("source", "file", "chain", cfg.Chain),
	}
}

func (s *FileSource) Name() string {
	return "file"
}

func (s *FileSource) Stream(ctx context.Context, events chan<- adapter.Event) error {
	s.logger.Info("starting file source stream",
		"fixtures_dir", s.cfg.FixturesDir,
		"chain", s.cfg.Chain,
		"loop", s.cfg.Loop,
		"playback_speed", s.cfg.PlaybackSpeed,
	)

	for {
		files, err := s.findFixtureFiles()
		if err != nil {
			return fmt.Errorf("find fixture files: %w", err)
		}

		if len(files) == 0 {
			s.logger.Warn("no fixture files found", "dir", s.cfg.FixturesDir)
			return nil
		}

		s.logger.Info("found fixture files", "count", len(files))

		var lastTimestamp int64
		for _, file := range files {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			fileEvents, timestamp, err := s.loadFixtureFile(file)
			if err != nil {
				s.logger.Warn("failed to load fixture", "file", file, "error", err)
				continue
			}

			if s.cfg.PlaybackSpeed > 0 && lastTimestamp > 0 && timestamp > lastTimestamp {
				delay := time.Duration(float64(timestamp-lastTimestamp)/s.cfg.PlaybackSpeed) * time.Second
				if delay > 0 && delay < 60*time.Second {
					s.logger.Debug("playback delay", "delay", delay)
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(delay):
					}
				}
			}
			lastTimestamp = timestamp

			for _, event := range fileEvents {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case events <- event:
					s.logger.Debug("sent event",
						"block", event.BlockNumber,
						"type", event.EventType,
					)
				}
			}
		}

		if !s.cfg.Loop {
			break
		}

		s.logger.Info("looping fixtures")
	}

	s.logger.Info("file source stream completed")
	return nil
}

func (s *FileSource) findFixtureFiles() ([]string, error) {
	var files []string

	err := filepath.Walk(s.cfg.FixturesDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".json" {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Strings(files)

	if s.cfg.Chain != "" {
		var filtered []string
		for _, f := range files {
			base := filepath.Base(f)
			switch s.cfg.Chain {
			case "solana":
				if len(base) > 7 && base[:7] == "solana_" {
					filtered = append(filtered, f)
				}
			case "evm", "ethereum":
				if len(base) >= 5 && (base[:5] == "block" || base[:4] == "logs" || base[:3] == "txs") {
					filtered = append(filtered, f)
				}
			default:
				filtered = append(filtered, f)
			}
		}
		files = filtered
	}

	return files, nil
}

func (s *FileSource) loadFixtureFile(path string) ([]adapter.Event, int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}

	var fixture Fixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		return nil, 0, fmt.Errorf("parse fixture: %w", err)
	}

	var events []adapter.Event
	var timestamp int64

	switch fixture.Chain {
	case "evm":
		events, timestamp, err = s.parseEVMFixture(fixture)
	case "solana":
		events, timestamp, err = s.parseSolanaFixture(fixture)
	default:
		return nil, 0, fmt.Errorf("unsupported chain: %s", fixture.Chain)
	}

	if err != nil {
		return nil, 0, fmt.Errorf("parse %s fixture: %w", fixture.Chain, err)
	}

	return events, timestamp, nil
}

func (s *FileSource) parseEVMFixture(fixture Fixture) ([]adapter.Event, int64, error) {
	var events []adapter.Event
	var timestamp int64

	switch fixture.Type {
	case "block":
		var block EVMBlockFixture
		if err := json.Unmarshal(fixture.Data, &block); err != nil {
			return nil, 0, err
		}
		timestamp = int64(block.Timestamp)

		events = append(events, adapter.Event{
			Chain:           "evm",
			CommitmentLevel: "finalized",
			BlockNumber:     block.Number,
			BlockHash:       block.Hash,
			ParentHash:      block.ParentHash,
			EventType:       "block",
			Timestamp:       timestamp,
		})

	case "logs":
		var logs []EVMLogFixture
		if err := json.Unmarshal(fixture.Data, &logs); err != nil {
			return nil, 0, err
		}

		for _, log := range logs {
			if log.Removed {
				continue
			}

			events = append(events, adapter.Event{
				Chain:           "evm",
				CommitmentLevel: "finalized",
				BlockNumber:     log.BlockNumber,
				BlockHash:       log.BlockHash,
				TxHash:          log.TxHash,
				TxIndex:         uint32(log.TxIndex),
				EventIndex:      uint32(log.LogIndex),
				EventType:       "log",
				Accounts:        []string{log.Address},
				ProgramID:       log.Address,
				Payload:         []byte(log.Data),
			})
		}

		if len(events) > 0 {
			timestamp = fixture.RecordedAt.Unix()
		}

	case "transactions":
		timestamp = fixture.RecordedAt.Unix()
	}

	return events, timestamp, nil
}

func (s *FileSource) parseSolanaFixture(fixture Fixture) ([]adapter.Event, int64, error) {
	var events []adapter.Event
	var timestamp int64

	switch fixture.Type {
	case "block":
		var block SolanaBlockFixture
		if err := json.Unmarshal(fixture.Data, &block); err != nil {
			return nil, 0, err
		}
		timestamp = block.BlockTime

		events = append(events, adapter.Event{
			Chain:           "solana",
			CommitmentLevel: "finalized",
			BlockNumber:     block.Slot,
			BlockHash:       block.Blockhash,
			ParentHash:      block.PreviousBlockhash,
			EventType:       "block",
			Timestamp:       timestamp,
		})

	case "transactions":
		var txs []SolanaTransactionFixture
		if err := json.Unmarshal(fixture.Data, &txs); err != nil {
			return nil, 0, err
		}

		for _, tx := range txs {
			if tx.Err != "" {
				continue
			}

			var programID string
			if len(tx.ProgramIDs) > 0 {
				programID = tx.ProgramIDs[0]
			}

			events = append(events, adapter.Event{
				Chain:           "solana",
				CommitmentLevel: "finalized",
				BlockNumber:     tx.Slot,
				TxHash:          tx.Signature,
				EventType:       "transaction",
				Accounts:        tx.Accounts,
				ProgramID:       programID,
				Timestamp:       tx.BlockTime,
				NativeValue:     tx.Fee,
			})
		}

		if len(events) > 0 && len(txs) > 0 {
			timestamp = txs[0].BlockTime
		}

	case "account":
		timestamp = fixture.RecordedAt.Unix()
	}

	return events, timestamp, nil
}

var _ adapter.Source = (*FileSource)(nil)
