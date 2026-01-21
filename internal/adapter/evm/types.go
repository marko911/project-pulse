package evm

import (
	"math/big"
	"time"
)

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
	Status      uint64
}

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

type BlockHeader struct {
	Number     uint64
	Hash       string
	ParentHash string
	Timestamp  time.Time
}

type CommitmentLevel int

const (
	CommitmentProcessed CommitmentLevel = iota
	CommitmentConfirmed
	CommitmentFinalized
)

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

func (c CommitmentLevel) TopicName(chain string) string {
	return "events." + chain + "." + c.String()
}
