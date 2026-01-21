package evm

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Chain string `yaml:"chain"`

	ChainID uint64 `yaml:"chain_id"`

	RPC RPCConfig `yaml:"rpc"`

	Broker BrokerConfig `yaml:"broker"`

	Processing ProcessingConfig `yaml:"processing"`

	Subscription SubscriptionConfig `yaml:"subscription"`
}

type SubscriptionConfig struct {
	Addresses []string `yaml:"addresses"`

	Topics [][]string `yaml:"topics"`
}

type RPCConfig struct {
	URL string `yaml:"url"`

	WSURL string `yaml:"ws_url"`

	FallbackURLs []string `yaml:"fallback_urls"`

	Timeout time.Duration `yaml:"timeout"`

	BlockPollInterval int `yaml:"block_poll_interval"`

	MaxRetries    int           `yaml:"max_retries"`
	RetryInterval time.Duration `yaml:"retry_interval"`
}

type BrokerConfig struct {
	Addresses []string `yaml:"addresses"`

	TopicPrefix string `yaml:"topic_prefix"`

	BatchSize    int           `yaml:"batch_size"`
	BatchTimeout time.Duration `yaml:"batch_timeout"`

	PartitionCount int `yaml:"partition_count"`

	PartitionKeyStrategy string `yaml:"partition_key_strategy"`
}

type ProcessingConfig struct {
	StartBlock uint64 `yaml:"start_block"`

	ProcessedDepth uint64 `yaml:"processed_depth"`
	ConfirmedDepth uint64 `yaml:"confirmed_depth"`
	FinalizedDepth uint64 `yaml:"finalized_depth"`

	EventTypes []string `yaml:"event_types"`

	WatchAddresses []string `yaml:"watch_addresses"`
}

func LoadConfig(configPath, chain, rpcURL string) (*Config, error) {
	cfg := &Config{
		Chain: chain,
		RPC: RPCConfig{
			Timeout:           30 * time.Second,
			MaxRetries:        3,
			RetryInterval:     5 * time.Second,
			BlockPollInterval: 1000,
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

	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("read config file: %w", err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse config file: %w", err)
		}
	}

	if rpcURL != "" {
		cfg.RPC.URL = rpcURL
	}
	if chain != "" {
		cfg.Chain = chain
	}

	cfg.ChainID = chainNameToID(cfg.Chain)

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
