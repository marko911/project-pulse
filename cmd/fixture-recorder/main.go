package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
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
	Number       uint64            `json:"number"`
	Hash         string            `json:"hash"`
	ParentHash   string            `json:"parent_hash"`
	Timestamp    uint64            `json:"timestamp"`
	GasLimit     uint64            `json:"gas_limit"`
	GasUsed      uint64            `json:"gas_used"`
	BaseFee      string            `json:"base_fee,omitempty"`
	Transactions []string          `json:"transactions"`
	Extra        map[string]string `json:"extra,omitempty"`
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

type EVMTransactionFixture struct {
	Hash        string `json:"hash"`
	Nonce       uint64 `json:"nonce"`
	BlockHash   string `json:"block_hash"`
	BlockNumber uint64 `json:"block_number"`
	TxIndex     uint   `json:"tx_index"`
	From        string `json:"from"`
	To          string `json:"to,omitempty"`
	Value       string `json:"value"`
	GasPrice    string `json:"gas_price,omitempty"`
	Gas         uint64 `json:"gas"`
	Input       string `json:"input"`
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

type SolanaAccountFixture struct {
	Pubkey     string `json:"pubkey"`
	Lamports   uint64 `json:"lamports"`
	Owner      string `json:"owner"`
	Executable bool   `json:"executable"`
	RentEpoch  string `json:"rent_epoch"`
	DataLen    int    `json:"data_len"`
	Data       string `json:"data,omitempty"`
}

type Config struct {
	Chain        string
	Endpoint     string
	OutputDir    string
	FixtureType  string
	BlockNumber  int64
	BlockCount   int
	ContractAddr string
	FromBlock    int64
	ToBlock      int64
}

func main() {
	chain := flag.String("chain", "evm", "Chain type: evm, solana")
	endpoint := flag.String("endpoint", "", "RPC endpoint URL")
	outputDir := flag.String("output", "./fixtures", "Output directory for fixtures")
	fixtureType := flag.String("type", "block", "Fixture type: block, logs, tx")
	blockNumber := flag.Int64("block", -1, "Block number to fetch (-1 for latest)")
	blockCount := flag.Int("count", 1, "Number of blocks to fetch")
	contractAddr := flag.String("contract", "", "Contract address for log filtering")
	fromBlock := flag.Int64("from", -1, "Start block for range queries (-1 for latest-count)")
	toBlock := flag.Int64("to", -1, "End block for range queries (-1 for latest)")
	logLevel := flag.String("log-level", "info", "Log level: debug, info, warn, error")
	flag.Parse()

	level := parseLogLevel(*logLevel)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	if *endpoint == "" {
		logger.Error("endpoint is required")
		flag.Usage()
		os.Exit(1)
	}

	cfg := Config{
		Chain:        *chain,
		Endpoint:     *endpoint,
		OutputDir:    *outputDir,
		FixtureType:  *fixtureType,
		BlockNumber:  *blockNumber,
		BlockCount:   *blockCount,
		ContractAddr: *contractAddr,
		FromBlock:    *fromBlock,
		ToBlock:      *toBlock,
	}

	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		logger.Error("failed to create output directory", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("received shutdown signal")
		cancel()
	}()

	logger.Info("starting fixture recorder",
		"chain", cfg.Chain,
		"type", cfg.FixtureType,
		"output", cfg.OutputDir,
	)

	var err error
	switch cfg.Chain {
	case "evm":
		err = recordEVM(ctx, cfg, logger)
	case "solana":
		err = recordSolana(ctx, cfg, logger)
	default:
		logger.Error("unsupported chain", "chain", cfg.Chain)
		os.Exit(1)
	}

	if err != nil {
		logger.Error("recording failed", "error", err)
		os.Exit(1)
	}

	logger.Info("fixture recording complete")
}

func recordEVM(ctx context.Context, cfg Config, logger *slog.Logger) error {
	client, err := ethclient.DialContext(ctx, cfg.Endpoint)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer client.Close()

	chainID, err := client.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("failed to get chain ID: %w", err)
	}
	logger.Info("connected to EVM chain", "chain_id", chainID)

	switch cfg.FixtureType {
	case "block":
		return recordEVMBlocks(ctx, client, cfg, logger)
	case "logs":
		return recordEVMLogs(ctx, client, cfg, logger)
	case "tx":
		return recordEVMTransactions(ctx, client, cfg, logger)
	default:
		return fmt.Errorf("unsupported fixture type: %s", cfg.FixtureType)
	}
}

func recordEVMBlocks(ctx context.Context, client *ethclient.Client, cfg Config, logger *slog.Logger) error {
	latestBlock, err := client.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest block: %w", err)
	}

	startBlock := uint64(cfg.BlockNumber)
	if cfg.BlockNumber < 0 {
		startBlock = latestBlock - uint64(cfg.BlockCount) + 1
	}

	logger.Info("fetching blocks",
		"start", startBlock,
		"count", cfg.BlockCount,
		"latest", latestBlock,
	)

	for i := 0; i < cfg.BlockCount; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		blockNum := startBlock + uint64(i)
		block, err := client.BlockByNumber(ctx, big.NewInt(int64(blockNum)))
		if err != nil {
			logger.Warn("failed to fetch block", "block", blockNum, "error", err)
			continue
		}

		txHashes := make([]string, len(block.Transactions()))
		for j, tx := range block.Transactions() {
			txHashes[j] = tx.Hash().Hex()
		}

		blockFixture := EVMBlockFixture{
			Number:       block.NumberU64(),
			Hash:         block.Hash().Hex(),
			ParentHash:   block.ParentHash().Hex(),
			Timestamp:    block.Time(),
			GasLimit:     block.GasLimit(),
			GasUsed:      block.GasUsed(),
			Transactions: txHashes,
		}

		if block.BaseFee() != nil {
			blockFixture.BaseFee = block.BaseFee().String()
		}

		data, err := json.Marshal(blockFixture)
		if err != nil {
			return fmt.Errorf("failed to marshal block: %w", err)
		}

		fixture := Fixture{
			Chain:       "evm",
			Type:        "block",
			RecordedAt:  time.Now().UTC(),
			BlockNumber: block.NumberU64(),
			BlockHash:   block.Hash().Hex(),
			Data:        data,
		}

		filename := filepath.Join(cfg.OutputDir, fmt.Sprintf("block_%d.json", blockNum))
		if err := saveFixture(filename, fixture); err != nil {
			return err
		}

		logger.Info("recorded block", "number", blockNum, "file", filename)
	}

	return nil
}

func recordEVMLogs(ctx context.Context, client *ethclient.Client, cfg Config, logger *slog.Logger) error {
	latestBlock, err := client.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest block: %w", err)
	}

	fromBlock := uint64(cfg.FromBlock)
	toBlock := uint64(cfg.ToBlock)

	if cfg.FromBlock < 0 {
		fromBlock = latestBlock - uint64(cfg.BlockCount)
	}
	if cfg.ToBlock < 0 {
		toBlock = latestBlock
	}

	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(fromBlock)),
		ToBlock:   big.NewInt(int64(toBlock)),
	}

	if cfg.ContractAddr != "" {
		query.Addresses = []common.Address{common.HexToAddress(cfg.ContractAddr)}
	}

	logger.Info("fetching logs",
		"from", fromBlock,
		"to", toBlock,
		"contract", cfg.ContractAddr,
	)

	logs, err := client.FilterLogs(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to fetch logs: %w", err)
	}

	logger.Info("found logs", "count", len(logs))

	logsByBlock := make(map[uint64][]EVMLogFixture)
	for _, log := range logs {
		topics := make([]string, len(log.Topics))
		for i, topic := range log.Topics {
			topics[i] = topic.Hex()
		}

		logFixture := EVMLogFixture{
			Address:     log.Address.Hex(),
			Topics:      topics,
			Data:        common.Bytes2Hex(log.Data),
			BlockNumber: log.BlockNumber,
			TxHash:      log.TxHash.Hex(),
			TxIndex:     log.TxIndex,
			BlockHash:   log.BlockHash.Hex(),
			LogIndex:    log.Index,
			Removed:     log.Removed,
		}

		logsByBlock[log.BlockNumber] = append(logsByBlock[log.BlockNumber], logFixture)
	}

	for blockNum, blockLogs := range logsByBlock {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		data, err := json.Marshal(blockLogs)
		if err != nil {
			return fmt.Errorf("failed to marshal logs: %w", err)
		}

		fixture := Fixture{
			Chain:       "evm",
			Type:        "logs",
			RecordedAt:  time.Now().UTC(),
			BlockNumber: blockNum,
			Data:        data,
		}

		filename := filepath.Join(cfg.OutputDir, fmt.Sprintf("logs_block_%d.json", blockNum))
		if err := saveFixture(filename, fixture); err != nil {
			return err
		}

		logger.Info("recorded logs", "block", blockNum, "count", len(blockLogs), "file", filename)
	}

	return nil
}

func recordEVMTransactions(ctx context.Context, client *ethclient.Client, cfg Config, logger *slog.Logger) error {
	latestBlock, err := client.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest block: %w", err)
	}

	startBlock := uint64(cfg.BlockNumber)
	if cfg.BlockNumber < 0 {
		startBlock = latestBlock - uint64(cfg.BlockCount) + 1
	}

	logger.Info("fetching transactions",
		"start", startBlock,
		"count", cfg.BlockCount,
	)

	for i := 0; i < cfg.BlockCount; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		blockNum := startBlock + uint64(i)
		block, err := client.BlockByNumber(ctx, big.NewInt(int64(blockNum)))
		if err != nil {
			logger.Warn("failed to fetch block", "block", blockNum, "error", err)
			continue
		}

		var txFixtures []EVMTransactionFixture
		chainID, _ := client.ChainID(ctx)
		signer := types.LatestSignerForChainID(chainID)

		for j, tx := range block.Transactions() {
			from, err := types.Sender(signer, tx)
			if err != nil {
				logger.Warn("failed to get tx sender", "tx", tx.Hash().Hex(), "error", err)
				from = common.Address{}
			}

			txFixture := EVMTransactionFixture{
				Hash:        tx.Hash().Hex(),
				Nonce:       tx.Nonce(),
				BlockHash:   block.Hash().Hex(),
				BlockNumber: block.NumberU64(),
				TxIndex:     uint(j),
				From:        from.Hex(),
				Value:       tx.Value().String(),
				Gas:         tx.Gas(),
				Input:       common.Bytes2Hex(tx.Data()),
			}

			if tx.To() != nil {
				txFixture.To = tx.To().Hex()
			}

			if tx.GasPrice() != nil {
				txFixture.GasPrice = tx.GasPrice().String()
			}

			txFixtures = append(txFixtures, txFixture)
		}

		if len(txFixtures) == 0 {
			logger.Debug("no transactions in block", "block", blockNum)
			continue
		}

		data, err := json.Marshal(txFixtures)
		if err != nil {
			return fmt.Errorf("failed to marshal transactions: %w", err)
		}

		fixture := Fixture{
			Chain:       "evm",
			Type:        "transactions",
			RecordedAt:  time.Now().UTC(),
			BlockNumber: block.NumberU64(),
			BlockHash:   block.Hash().Hex(),
			Data:        data,
		}

		filename := filepath.Join(cfg.OutputDir, fmt.Sprintf("txs_block_%d.json", blockNum))
		if err := saveFixture(filename, fixture); err != nil {
			return err
		}

		logger.Info("recorded transactions", "block", blockNum, "count", len(txFixtures), "file", filename)
	}

	return nil
}

func recordSolana(ctx context.Context, cfg Config, logger *slog.Logger) error {
	client := rpc.New(cfg.Endpoint)

	version, err := client.GetVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to Solana RPC: %w", err)
	}
	logger.Info("connected to Solana RPC", "version", version.SolanaCore)

	switch cfg.FixtureType {
	case "block":
		return recordSolanaBlocks(ctx, client, cfg, logger)
	case "tx":
		return recordSolanaTransactions(ctx, client, cfg, logger)
	case "account":
		return recordSolanaAccounts(ctx, client, cfg, logger)
	default:
		return fmt.Errorf("unsupported fixture type for Solana: %s (use: block, tx, account)", cfg.FixtureType)
	}
}

func recordSolanaBlocks(ctx context.Context, client *rpc.Client, cfg Config, logger *slog.Logger) error {
	slot, err := client.GetSlot(ctx, rpc.CommitmentFinalized)
	if err != nil {
		return fmt.Errorf("failed to get current slot: %w", err)
	}

	startSlot := uint64(cfg.BlockNumber)
	if cfg.BlockNumber < 0 {
		startSlot = slot - uint64(cfg.BlockCount) + 1
	}

	logger.Info("fetching Solana blocks",
		"start_slot", startSlot,
		"count", cfg.BlockCount,
		"current_slot", slot,
	)

	maxSupportedVersion := uint64(0)
	for i := 0; i < cfg.BlockCount; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		targetSlot := startSlot + uint64(i)
		block, err := client.GetBlockWithOpts(ctx, targetSlot, &rpc.GetBlockOpts{
			Encoding:                       solana.EncodingBase64,
			TransactionDetails:             rpc.TransactionDetailsFull,
			MaxSupportedTransactionVersion: &maxSupportedVersion,
			Commitment:                     rpc.CommitmentFinalized,
		})
		if err != nil {
			logger.Warn("failed to fetch block", "slot", targetSlot, "error", err)
			continue
		}

		if block == nil {
			logger.Debug("slot skipped (no block)", "slot", targetSlot)
			continue
		}

		txSigs := make([]string, 0, len(block.Transactions))
		for _, tx := range block.Transactions {
			if tx.Transaction != nil {
				parsed, err := tx.GetTransaction()
				if err == nil && parsed != nil {
					for _, sig := range parsed.Signatures {
						txSigs = append(txSigs, sig.String())
					}
				}
			}
		}

		blockFixture := SolanaBlockFixture{
			Slot:              targetSlot,
			Blockhash:         block.Blockhash.String(),
			PreviousBlockhash: block.PreviousBlockhash.String(),
			ParentSlot:        block.ParentSlot,
			Transactions:      txSigs,
		}

		if block.BlockTime != nil {
			blockFixture.BlockTime = int64(*block.BlockTime)
		}
		if block.BlockHeight != nil {
			blockFixture.BlockHeight = *block.BlockHeight
		}

		data, err := json.Marshal(blockFixture)
		if err != nil {
			return fmt.Errorf("failed to marshal block: %w", err)
		}

		fixture := Fixture{
			Chain:       "solana",
			Type:        "block",
			RecordedAt:  time.Now().UTC(),
			BlockNumber: targetSlot,
			BlockHash:   block.Blockhash.String(),
			Data:        data,
		}

		filename := filepath.Join(cfg.OutputDir, fmt.Sprintf("solana_block_%d.json", targetSlot))
		if err := saveFixture(filename, fixture); err != nil {
			return err
		}

		logger.Info("recorded Solana block", "slot", targetSlot, "txs", len(txSigs), "file", filename)
	}

	return nil
}

func recordSolanaTransactions(ctx context.Context, client *rpc.Client, cfg Config, logger *slog.Logger) error {
	slot, err := client.GetSlot(ctx, rpc.CommitmentFinalized)
	if err != nil {
		return fmt.Errorf("failed to get current slot: %w", err)
	}

	startSlot := uint64(cfg.BlockNumber)
	if cfg.BlockNumber < 0 {
		startSlot = slot - uint64(cfg.BlockCount) + 1
	}

	logger.Info("fetching Solana transactions",
		"start_slot", startSlot,
		"count", cfg.BlockCount,
	)

	maxSupportedVersion := uint64(0)
	for i := 0; i < cfg.BlockCount; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		targetSlot := startSlot + uint64(i)
		block, err := client.GetBlockWithOpts(ctx, targetSlot, &rpc.GetBlockOpts{
			Encoding:                       solana.EncodingBase64,
			TransactionDetails:             rpc.TransactionDetailsFull,
			MaxSupportedTransactionVersion: &maxSupportedVersion,
			Commitment:                     rpc.CommitmentFinalized,
		})
		if err != nil {
			logger.Warn("failed to fetch block", "slot", targetSlot, "error", err)
			continue
		}

		if block == nil || len(block.Transactions) == 0 {
			continue
		}

		var txFixtures []SolanaTransactionFixture
		for _, txWithMeta := range block.Transactions {
			if txWithMeta.Transaction == nil {
				continue
			}

			parsed, err := txWithMeta.GetTransaction()
			if err != nil || parsed == nil {
				continue
			}

			sig := ""
			if len(parsed.Signatures) > 0 {
				sig = parsed.Signatures[0].String()
			}

			accounts := make([]string, len(parsed.Message.AccountKeys))
			for j, acc := range parsed.Message.AccountKeys {
				accounts[j] = acc.String()
			}

			programIDs := make([]string, 0)
			seenPrograms := make(map[string]bool)
			for _, inst := range parsed.Message.Instructions {
				if int(inst.ProgramIDIndex) < len(parsed.Message.AccountKeys) {
					progID := parsed.Message.AccountKeys[inst.ProgramIDIndex].String()
					if !seenPrograms[progID] {
						programIDs = append(programIDs, progID)
						seenPrograms[progID] = true
					}
				}
			}

			txFixture := SolanaTransactionFixture{
				Signature:  sig,
				Slot:       targetSlot,
				Accounts:   accounts,
				ProgramIDs: programIDs,
			}

			if block.BlockTime != nil {
				txFixture.BlockTime = int64(*block.BlockTime)
			}

			if txWithMeta.Meta != nil {
				txFixture.Fee = txWithMeta.Meta.Fee
				if txWithMeta.Meta.Err != nil {
					txFixture.Err = fmt.Sprintf("%v", txWithMeta.Meta.Err)
				}
				txFixture.LogMessages = txWithMeta.Meta.LogMessages
			}

			txFixtures = append(txFixtures, txFixture)
		}

		if len(txFixtures) == 0 {
			continue
		}

		data, err := json.Marshal(txFixtures)
		if err != nil {
			return fmt.Errorf("failed to marshal transactions: %w", err)
		}

		fixture := Fixture{
			Chain:       "solana",
			Type:        "transactions",
			RecordedAt:  time.Now().UTC(),
			BlockNumber: targetSlot,
			BlockHash:   block.Blockhash.String(),
			Data:        data,
		}

		filename := filepath.Join(cfg.OutputDir, fmt.Sprintf("solana_txs_slot_%d.json", targetSlot))
		if err := saveFixture(filename, fixture); err != nil {
			return err
		}

		logger.Info("recorded Solana transactions", "slot", targetSlot, "count", len(txFixtures), "file", filename)
	}

	return nil
}

func recordSolanaAccounts(ctx context.Context, client *rpc.Client, cfg Config, logger *slog.Logger) error {
	if cfg.ContractAddr == "" {
		return fmt.Errorf("account address required for Solana account recording (use -contract flag)")
	}

	pubkey, err := solana.PublicKeyFromBase58(cfg.ContractAddr)
	if err != nil {
		return fmt.Errorf("invalid Solana public key: %w", err)
	}

	logger.Info("fetching Solana account", "pubkey", cfg.ContractAddr)

	account, err := client.GetAccountInfoWithOpts(ctx, pubkey, &rpc.GetAccountInfoOpts{
		Encoding:   solana.EncodingBase64,
		Commitment: rpc.CommitmentFinalized,
	})
	if err != nil {
		return fmt.Errorf("failed to fetch account: %w", err)
	}

	if account == nil || account.Value == nil {
		return fmt.Errorf("account not found: %s", cfg.ContractAddr)
	}

	accData := account.Value

	rentEpoch := ""
	if accData.RentEpoch != nil {
		rentEpoch = accData.RentEpoch.String()
	}

	binaryData := accData.Data.GetBinary()
	accountFixture := SolanaAccountFixture{
		Pubkey:     cfg.ContractAddr,
		Lamports:   accData.Lamports,
		Owner:      accData.Owner.String(),
		Executable: accData.Executable,
		RentEpoch:  rentEpoch,
		DataLen:    len(binaryData),
	}

	if len(binaryData) <= 10240 {
		accountFixture.Data = base64.StdEncoding.EncodeToString(binaryData)
	}

	data, err := json.Marshal(accountFixture)
	if err != nil {
		return fmt.Errorf("failed to marshal account: %w", err)
	}

	slot, _ := client.GetSlot(ctx, rpc.CommitmentFinalized)

	fixture := Fixture{
		Chain:       "solana",
		Type:        "account",
		RecordedAt:  time.Now().UTC(),
		BlockNumber: slot,
		Data:        data,
	}

	filename := filepath.Join(cfg.OutputDir, fmt.Sprintf("solana_account_%s.json", cfg.ContractAddr[:8]))
	if err := saveFixture(filename, fixture); err != nil {
		return err
	}

	logger.Info("recorded Solana account",
		"pubkey", cfg.ContractAddr,
		"lamports", accData.Lamports,
		"owner", accData.Owner.String(),
		"file", filename,
	)

	return nil
}

func saveFixture(filename string, fixture Fixture) error {
	data, err := json.MarshalIndent(fixture, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal fixture: %w", err)
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("failed to write fixture: %w", err)
	}

	return nil
}

func parseLogLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
