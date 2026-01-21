package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type TopicConfig struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	RetentionMs       int64
	CleanupPolicy     string
}

func DefaultTopicConfigs() []TopicConfig {
	return []TopicConfig{
		{
			Name:              "raw-events",
			Partitions:        12,
			ReplicationFactor: 1,
			RetentionMs:       7 * 24 * 60 * 60 * 1000,
			CleanupPolicy:     "delete",
		},
		{
			Name:              "canonical-events",
			Partitions:        24,
			ReplicationFactor: 1,
			RetentionMs:       7 * 24 * 60 * 60 * 1000,
			CleanupPolicy:     "delete",
		},
		{
			Name:              "function-invocations",
			Partitions:        16,
			ReplicationFactor: 1,
			RetentionMs:       24 * 60 * 60 * 1000,
			CleanupPolicy:     "delete",
		},
		{
			Name:              "function-results",
			Partitions:        8,
			ReplicationFactor: 1,
			RetentionMs:       3 * 24 * 60 * 60 * 1000,
			CleanupPolicy:     "delete",
		},
		{
			Name:              "billing-events",
			Partitions:        4,
			ReplicationFactor: 1,
			RetentionMs:       30 * 24 * 60 * 60 * 1000,
			CleanupPolicy:     "delete",
		},
		{
			Name:              "dlq-function-invocations",
			Partitions:        4,
			ReplicationFactor: 1,
			RetentionMs:       7 * 24 * 60 * 60 * 1000,
			CleanupPolicy:     "delete",
		},
	}
}

type TopicManager struct {
	admin *kadm.Client
}

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

func (m *TopicManager) EnsureTopics(ctx context.Context, configs []TopicConfig) error {
	existing, err := m.admin.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("list topics: %w", err)
	}

	existingSet := make(map[string]bool)
	for _, t := range existing {
		existingSet[t.Topic] = true
	}

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

func (m *TopicManager) Close() {
	m.admin.Close()
}

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
