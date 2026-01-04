// Package kafka provides Kafka/Redpanda utilities for topic management.
package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TopicConfig defines the configuration for a Kafka topic.
type TopicConfig struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	RetentionMs       int64
	CleanupPolicy     string
}

// DefaultTopicConfigs returns the default topic configurations for Mirador.
func DefaultTopicConfigs() []TopicConfig {
	return []TopicConfig{
		{
			Name:              "raw-events",
			Partitions:        12,
			ReplicationFactor: 1,
			RetentionMs:       7 * 24 * 60 * 60 * 1000, // 7 days
			CleanupPolicy:     "delete",
		},
		{
			Name:              "canonical-events",
			Partitions:        24,
			ReplicationFactor: 1,
			RetentionMs:       7 * 24 * 60 * 60 * 1000, // 7 days
			CleanupPolicy:     "delete",
		},
		{
			Name:              "function-invocations",
			Partitions:        16,
			ReplicationFactor: 1,
			RetentionMs:       24 * 60 * 60 * 1000, // 1 day
			CleanupPolicy:     "delete",
		},
		{
			Name:              "function-results",
			Partitions:        8,
			ReplicationFactor: 1,
			RetentionMs:       3 * 24 * 60 * 60 * 1000, // 3 days
			CleanupPolicy:     "delete",
		},
		{
			Name:              "billing-events",
			Partitions:        4,
			ReplicationFactor: 1,
			RetentionMs:       30 * 24 * 60 * 60 * 1000, // 30 days
			CleanupPolicy:     "delete",
		},
		{
			Name:              "dlq-function-invocations",
			Partitions:        4,
			ReplicationFactor: 1,
			RetentionMs:       7 * 24 * 60 * 60 * 1000, // 7 days
			CleanupPolicy:     "delete",
		},
	}
}

// TopicManager manages Kafka topics.
type TopicManager struct {
	admin *kadm.Client
}

// NewTopicManager creates a new TopicManager.
func NewTopicManager(brokers string) (*TopicManager, error) {
	brokerList := strings.Split(brokers, ",")
	for i := range brokerList {
		brokerList[i] = strings.TrimSpace(brokerList[i])
	}

	client, err := kgo.NewClient(kgo.SeedBrokers(brokerList...))
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	return &TopicManager{
		admin: kadm.NewClient(client),
	}, nil
}

// EnsureTopics creates topics if they don't exist.
func (m *TopicManager) EnsureTopics(ctx context.Context, configs []TopicConfig) error {
	// List existing topics
	existing, err := m.admin.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("list topics: %w", err)
	}

	existingSet := make(map[string]bool)
	for _, t := range existing {
		existingSet[t.Topic] = true
	}

	// Create missing topics
	for _, cfg := range configs {
		if existingSet[cfg.Name] {
			continue
		}

		if err := m.CreateTopic(ctx, cfg); err != nil {
			return fmt.Errorf("create topic %s: %w", cfg.Name, err)
		}
	}

	return nil
}

// CreateTopic creates a single topic with the given configuration.
func (m *TopicManager) CreateTopic(ctx context.Context, cfg TopicConfig) error {
	resp, err := m.admin.CreateTopics(ctx, cfg.Partitions, cfg.ReplicationFactor,
		map[string]*string{
			"retention.ms":   stringPtr(fmt.Sprintf("%d", cfg.RetentionMs)),
			"cleanup.policy": stringPtr(cfg.CleanupPolicy),
		},
		cfg.Name,
	)
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}

	for _, r := range resp {
		if r.Err != nil {
			return fmt.Errorf("create topic %s: %w", r.Topic, r.Err)
		}
	}

	return nil
}

// ListTopics returns all topics.
func (m *TopicManager) ListTopics(ctx context.Context) ([]string, error) {
	topics, err := m.admin.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}

	names := make([]string, 0, len(topics))
	for _, t := range topics {
		names = append(names, t.Topic)
	}

	return names, nil
}

// Close releases resources.
func (m *TopicManager) Close() {
	m.admin.Close()
}

// WaitForTopic waits for a topic to be available.
func (m *TopicManager) WaitForTopic(ctx context.Context, topic string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		topics, err := m.admin.ListTopics(ctx, topic)
		if err == nil && len(topics) > 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}

	return fmt.Errorf("timeout waiting for topic %s", topic)
}

func stringPtr(s string) *string {
	return &s
}
