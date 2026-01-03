// Package evm provides the EVM blockchain adapter for ingesting events from
// Ethereum and EVM-compatible chains.
package evm

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds the configuration for the EVM adapter.
type Config struct {
	// Chain identifier (ethereum, polygon, arbitrum, etc.)
	Chain string `yaml:"chain"`

	// ChainID is the numeric chain ID
	ChainID uint64 `yaml:"chain_id"`

	// RPC endpoint configuration
	RPC RPCConfig `yaml:"rpc"`

	// Broker configuration for publishing events
	Broker BrokerConfig `yaml:"broker"`

	// Processing options
	Processing ProcessingConfig `yaml:"processing"`

	// Subscription filters
	Subscription SubscriptionConfig `yaml:"subscription"`
}

// SubscriptionConfig holds event subscription filters.
type SubscriptionConfig struct {
	// Contract addresses to filter (empty = all)
	Addresses []string `yaml:"addresses"`

	// Topic filters (array of topic groups for OR matching)
	Topics [][]string `yaml:"topics"`
}

// RPCConfig holds RPC connection settings.
type RPCConfig struct {
	// Primary HTTP endpoint URL
	URL string `yaml:"url"`

	// WebSocket endpoint URL (for subscriptions)
	WSURL string `yaml:"ws_url"`

	// Fallback endpoints for redundancy
	FallbackURLs []string `yaml:"fallback_urls"`

	// Connection timeout
	Timeout time.Duration `yaml:"timeout"`

	// Block polling interval (ms) for HTTP mode
	BlockPollInterval int `yaml:"block_poll_interval"`

	// Retry settings
	MaxRetries    int           `yaml:"max_retries"`
	RetryInterval time.Duration `yaml:"retry_interval"`
}

// BrokerConfig holds message broker settings.
type BrokerConfig struct {
	// Broker addresses (Redpanda/Kafka)
	Addresses []string `yaml:"addresses"`

	// Topic prefix for this chain
	TopicPrefix string `yaml:"topic_prefix"`

	// Producer settings
	BatchSize    int           `yaml:"batch_size"`
	BatchTimeout time.Duration `yaml:"batch_timeout"`

	// Partition configuration
	// PartitionCount is the number of partitions for the topic (0 = use broker default)
	PartitionCount int `yaml:"partition_count"`
	// PartitionKeyStrategy controls how events are distributed across partitions.
	// Options: "chain_block" (default - chain:blockNumber), "account" (by first account),
	// "event_type" (by event type), "round_robin" (no key, round-robin distribution)
	PartitionKeyStrategy string `yaml:"partition_key_strategy"`
}

// ProcessingConfig holds event processing settings.
type ProcessingConfig struct {
	// Starting block (0 = latest)
	StartBlock uint64 `yaml:"start_block"`

	// Confirmation depth for each commitment level
	ProcessedDepth uint64 `yaml:"processed_depth"`
	ConfirmedDepth uint64 `yaml:"confirmed_depth"`
	FinalizedDepth uint64 `yaml:"finalized_depth"`

	// Event types to process (empty = all)
	EventTypes []string `yaml:"event_types"`

	// Accounts/contracts to filter (empty = all)
	WatchAddresses []string `yaml:"watch_addresses"`
}

// LoadConfig loads configuration from file and/or environment.
func LoadConfig(configPath, chain, rpcURL string) (*Config, error) {
	cfg := &Config{
		Chain: chain,
		RPC: RPCConfig{
			Timeout:           30 * time.Second,
			MaxRetries:        3,
			RetryInterval:     5 * time.Second,
			BlockPollInterval: 1000, // 1 second
		},
		Broker: BrokerConfig{
			Addresses:    []string{"localhost:9092"},
			BatchSize:    100,
			BatchTimeout: 100 * time.Millisecond,
		},
		Processing: ProcessingConfig{
			ProcessedDepth: 0,
			ConfirmedDepth: 6,
			FinalizedDepth: 64,
		},
		Subscription: SubscriptionConfig{
			Addresses: []string{},
			Topics:    [][]string{},
		},
	}

	// Load from file if provided
	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("read config file: %w", err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse config file: %w", err)
		}
	}

	// Override with CLI flags
	if rpcURL != "" {
		cfg.RPC.URL = rpcURL
	}
	if chain != "" {
		cfg.Chain = chain
	}

	// Set chain ID based on chain name
	cfg.ChainID = chainNameToID(cfg.Chain)

	// Set topic prefix
	if cfg.Broker.TopicPrefix == "" {
		cfg.Broker.TopicPrefix = fmt.Sprintf("events.%s", cfg.Chain)
	}

	return cfg, nil
}

func chainNameToID(chain string) uint64 {
	switch chain {
	case "ethereum":
		return 1
	case "polygon":
		return 137
	case "arbitrum":
		return 42161
	case "optimism":
		return 10
	case "base":
		return 8453
	case "avalanche":
		return 43114
	case "bsc":
		return 56
	default:
		return 0
	}
}
