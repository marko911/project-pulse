package protov1

import (
	"time"
)

type CommitmentLevel int32

const (
	CommitmentLevel_COMMITMENT_LEVEL_UNSPECIFIED CommitmentLevel = 0
	CommitmentLevel_COMMITMENT_LEVEL_PROCESSED   CommitmentLevel = 1
	CommitmentLevel_COMMITMENT_LEVEL_CONFIRMED   CommitmentLevel = 2
	CommitmentLevel_COMMITMENT_LEVEL_FINALIZED   CommitmentLevel = 3
)

type ReorgAction int32

const (
	ReorgAction_REORG_ACTION_UNSPECIFIED ReorgAction = 0
	ReorgAction_REORG_ACTION_NORMAL      ReorgAction = 1
	ReorgAction_REORG_ACTION_RETRACT     ReorgAction = 2
	ReorgAction_REORG_ACTION_REPLACE     ReorgAction = 3
)

type Chain int32

const (
	Chain_CHAIN_UNSPECIFIED Chain = 0
	Chain_CHAIN_SOLANA      Chain = 1
	Chain_CHAIN_ETHEREUM    Chain = 2
	Chain_CHAIN_POLYGON     Chain = 3
	Chain_CHAIN_ARBITRUM    Chain = 4
	Chain_CHAIN_OPTIMISM    Chain = 5
	Chain_CHAIN_BASE        Chain = 6
	Chain_CHAIN_AVALANCHE   Chain = 7
	Chain_CHAIN_BSC         Chain = 8
)

type CanonicalEvent struct {
	EventId          string          `json:"event_id"`
	Chain            Chain           `json:"chain"`
	CommitmentLevel  CommitmentLevel `json:"commitment_level"`
	BlockNumber      uint64          `json:"block_number"`
	BlockHash        string          `json:"block_hash"`
	TxHash           string          `json:"tx_hash"`
	TxIndex          uint32          `json:"tx_index"`
	EventIndex       uint32          `json:"event_index"`
	EventType        string          `json:"event_type"`
	Accounts         []string        `json:"accounts"`
	Timestamp        time.Time       `json:"timestamp"`
	Payload          []byte          `json:"payload"`
	ReorgAction      ReorgAction     `json:"reorg_action"`
	SchemaVersion    uint32          `json:"schema_version"`
	IngestedAt       time.Time       `json:"ingested_at"`
	ReplacesEventId  string          `json:"replaces_event_id"`
	ProgramId        string          `json:"program_id"`
	NativeValue      uint64          `json:"native_value"`
}

type EventBatch struct {
	Events          []*CanonicalEvent
	Chain           Chain
	CommitmentLevel CommitmentLevel
	FromBlock       uint64
	ToBlock         uint64
}

type Manifest struct {
	Chain              Chain
	BlockNumber        uint64
	BlockHash          string
	ParentHash         string
	ExpectedTxCount    uint32
	ExpectedEventCount uint32
	EmittedTxCount     uint32
	EmittedEventCount  uint32
	EventIdsHash       string
	SourcesUsed        []string
	BlockTimestamp     time.Time
	IngestedAt         time.Time
	ManifestCreatedAt  time.Time
}
