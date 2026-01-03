package evm

import (
	"math/big"
	"time"
)

// Block represents an EVM block with relevant fields for event processing.
type Block struct {
	Number       uint64
	Hash         string
	ParentHash   string
	Timestamp    time.Time
	Transactions []Transaction
	LogsBloom    []byte
	GasUsed      uint64
	GasLimit     uint64
	BaseFee      *big.Int
}

// Transaction represents an EVM transaction.
type Transaction struct {
	Hash        string
	Index       uint32
	From        string
	To          string
	Value       *big.Int
	Gas         uint64
	GasPrice    *big.Int
	Input       []byte
	Nonce       uint64
	BlockNumber uint64
	BlockHash   string
	Logs        []Log
	Status      uint64 // 1 = success, 0 = failure
}

// Log represents an EVM event log.
type Log struct {
	Address     string
	Topics      []string
	Data        []byte
	BlockNumber uint64
	TxHash      string
	TxIndex     uint32
	BlockHash   string
	LogIndex    uint32
	Removed     bool
}

// BlockHeader is a lightweight block representation for tracking.
type BlockHeader struct {
	Number     uint64
	Hash       string
	ParentHash string
	Timestamp  time.Time
}

// CommitmentLevel represents the finality state of a block.
type CommitmentLevel int

const (
	CommitmentProcessed CommitmentLevel = iota
	CommitmentConfirmed
	CommitmentFinalized
)

// String returns the string representation of the commitment level.
func (c CommitmentLevel) String() string {
	switch c {
	case CommitmentProcessed:
		return "processed"
	case CommitmentConfirmed:
		return "confirmed"
	case CommitmentFinalized:
		return "finalized"
	default:
		return "unknown"
	}
}

// TopicName returns the Kafka/Redpanda topic name for this commitment level.
func (c CommitmentLevel) TopicName(chain string) string {
	return "events." + chain + "." + c.String()
}
