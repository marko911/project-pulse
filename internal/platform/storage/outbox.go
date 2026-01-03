package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	protov1 "github.com/mirador/pulse/pkg/proto/v1"
)

// OutboxRepository handles persistence of events and outbox messages.
type OutboxRepository struct {
	db *DB
}

// NewOutboxRepository creates a new OutboxRepository.
func NewOutboxRepository(db *DB) *OutboxRepository {
	return &OutboxRepository{db: db}
}

// SaveEventWithOutbox atomically saves a canonical event and its outbox message.
// This is the core of the transactional outbox pattern - both records are written
// in a single transaction to guarantee exactly-once delivery semantics.
func (r *OutboxRepository) SaveEventWithOutbox(ctx context.Context, event *protov1.CanonicalEvent, topic string) error {
	return r.db.WithTx(ctx, func(tx pgx.Tx) error {
		// Convert accounts slice to JSON for storage
		accountsJSON, err := json.Marshal(event.Accounts)
		if err != nil {
			return fmt.Errorf("marshal accounts: %w", err)
		}

		// Serialize the full event for the outbox payload
		// In production, this would use protobuf serialization
		payloadJSON, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshal payload: %w", err)
		}

		// Insert or update the event
		eventSQL := `
			INSERT INTO events (
				event_id, chain, block_number, block_hash, block_timestamp,
				tx_hash, tx_index, event_index, event_type, program_id,
				accounts, payload, commitment_level, reorg_action,
				replaces_event_id, native_value, schema_version, ingested_at
			) VALUES (
				$1, $2, $3, $4, $5,
				$6, $7, $8, $9, $10,
				$11, $12, $13, $14,
				$15, $16, $17, $18
			)
			ON CONFLICT (event_id, block_timestamp) DO UPDATE SET
				commitment_level = EXCLUDED.commitment_level,
				reorg_action = EXCLUDED.reorg_action,
				replaces_event_id = EXCLUDED.replaces_event_id,
				ingested_at = EXCLUDED.ingested_at
		`

		var programID *string
		if event.ProgramId != "" {
			programID = &event.ProgramId
		}

		var replacesEventID *string
		if event.ReplacesEventId != "" {
			replacesEventID = &event.ReplacesEventId
		}

		_, err = tx.Exec(ctx, eventSQL,
			event.EventId,
			int16(event.Chain),
			int64(event.BlockNumber),
			event.BlockHash,
			event.Timestamp,
			event.TxHash,
			int32(event.TxIndex),
			int32(event.EventIndex),
			event.EventType,
			programID,
			accountsJSON,
			event.Payload,
			int16(event.CommitmentLevel),
			int16(event.ReorgAction),
			replacesEventID,
			int64(event.NativeValue),
			int32(event.SchemaVersion),
			event.IngestedAt,
		)
		if err != nil {
			return fmt.Errorf("insert event: %w", err)
		}

		// Insert the outbox message
		outboxSQL := `
			INSERT INTO outbox (
				event_id, topic, partition_key, payload, chain, event_type
			) VALUES ($1, $2, $3, $4, $5, $6)
		`

		// Partition key ensures ordering within a chain
		partitionKey := fmt.Sprintf("%d:%d", event.Chain, event.BlockNumber)

		_, err = tx.Exec(ctx, outboxSQL,
			event.EventId,
			topic,
			partitionKey,
			payloadJSON,
			int16(event.Chain),
			event.EventType,
		)
		if err != nil {
			return fmt.Errorf("insert outbox: %w", err)
		}

		return nil
	})
}

// SaveBatchWithOutbox atomically saves multiple events with their outbox messages.
// All events are written in a single transaction.
func (r *OutboxRepository) SaveBatchWithOutbox(ctx context.Context, events []*protov1.CanonicalEvent, topic string) error {
	if len(events) == 0 {
		return nil
	}

	return r.db.WithTx(ctx, func(tx pgx.Tx) error {
		for _, event := range events {
			accountsJSON, err := json.Marshal(event.Accounts)
			if err != nil {
				return fmt.Errorf("marshal accounts for %s: %w", event.EventId, err)
			}

			payloadJSON, err := json.Marshal(event)
			if err != nil {
				return fmt.Errorf("marshal payload for %s: %w", event.EventId, err)
			}

			var programID *string
			if event.ProgramId != "" {
				programID = &event.ProgramId
			}

			var replacesEventID *string
			if event.ReplacesEventId != "" {
				replacesEventID = &event.ReplacesEventId
			}

			// Upsert event
			eventSQL := `
				INSERT INTO events (
					event_id, chain, block_number, block_hash, block_timestamp,
					tx_hash, tx_index, event_index, event_type, program_id,
					accounts, payload, commitment_level, reorg_action,
					replaces_event_id, native_value, schema_version, ingested_at
				) VALUES (
					$1, $2, $3, $4, $5,
					$6, $7, $8, $9, $10,
					$11, $12, $13, $14,
					$15, $16, $17, $18
				)
				ON CONFLICT (event_id, block_timestamp) DO UPDATE SET
					commitment_level = EXCLUDED.commitment_level,
					reorg_action = EXCLUDED.reorg_action,
					replaces_event_id = EXCLUDED.replaces_event_id,
					ingested_at = EXCLUDED.ingested_at
			`

			_, err = tx.Exec(ctx, eventSQL,
				event.EventId,
				int16(event.Chain),
				int64(event.BlockNumber),
				event.BlockHash,
				event.Timestamp,
				event.TxHash,
				int32(event.TxIndex),
				int32(event.EventIndex),
				event.EventType,
				programID,
				accountsJSON,
				event.Payload,
				int16(event.CommitmentLevel),
				int16(event.ReorgAction),
				replacesEventID,
				int64(event.NativeValue),
				int32(event.SchemaVersion),
				event.IngestedAt,
			)
			if err != nil {
				return fmt.Errorf("insert event %s: %w", event.EventId, err)
			}

			// Insert outbox message
			outboxSQL := `
				INSERT INTO outbox (
					event_id, topic, partition_key, payload, chain, event_type
				) VALUES ($1, $2, $3, $4, $5, $6)
			`

			partitionKey := fmt.Sprintf("%d:%d", event.Chain, event.BlockNumber)

			_, err = tx.Exec(ctx, outboxSQL,
				event.EventId,
				topic,
				partitionKey,
				payloadJSON,
				int16(event.Chain),
				event.EventType,
			)
			if err != nil {
				return fmt.Errorf("insert outbox for %s: %w", event.EventId, err)
			}
		}

		return nil
	})
}

// FetchPendingMessages retrieves pending outbox messages for publishing.
// Messages are returned in order by ID to maintain strict ordering.
func (r *OutboxRepository) FetchPendingMessages(ctx context.Context, limit int) ([]OutboxMessage, error) {
	sql := `
		SELECT id, event_id, topic, partition_key, payload, chain, event_type,
		       status, retry_count, max_retries, last_error,
		       created_at, processed_at, published_at
		FROM outbox
		WHERE status = 'pending'
		ORDER BY id ASC
		LIMIT $1
	`

	rows, err := r.db.pool.Query(ctx, sql, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending: %w", err)
	}
	defer rows.Close()

	var messages []OutboxMessage
	for rows.Next() {
		var msg OutboxMessage
		err := rows.Scan(
			&msg.ID, &msg.EventID, &msg.Topic, &msg.PartitionKey, &msg.Payload,
			&msg.Chain, &msg.EventType, &msg.Status, &msg.RetryCount, &msg.MaxRetries,
			&msg.LastError, &msg.CreatedAt, &msg.ProcessedAt, &msg.PublishedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		messages = append(messages, msg)
	}

	return messages, rows.Err()
}

// MarkAsProcessing atomically marks messages as processing.
// Returns the IDs that were successfully claimed (handles concurrent workers).
func (r *OutboxRepository) MarkAsProcessing(ctx context.Context, ids []int64) ([]int64, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	sql := `
		UPDATE outbox
		SET status = 'processing', processed_at = $1
		WHERE id = ANY($2) AND status = 'pending'
		RETURNING id
	`

	rows, err := r.db.pool.Query(ctx, sql, time.Now().UTC(), ids)
	if err != nil {
		return nil, fmt.Errorf("mark processing: %w", err)
	}
	defer rows.Close()

	var claimed []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan id: %w", err)
		}
		claimed = append(claimed, id)
	}

	return claimed, rows.Err()
}

// MarkAsPublished marks messages as successfully published.
func (r *OutboxRepository) MarkAsPublished(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	sql := `
		UPDATE outbox
		SET status = 'published', published_at = $1
		WHERE id = ANY($2)
	`

	_, err := r.db.pool.Exec(ctx, sql, time.Now().UTC(), ids)
	if err != nil {
		return fmt.Errorf("mark published: %w", err)
	}

	return nil
}

// MarkAsFailed marks messages as failed with an error message.
func (r *OutboxRepository) MarkAsFailed(ctx context.Context, id int64, errMsg string) error {
	sql := `
		UPDATE outbox
		SET status = CASE
				WHEN retry_count + 1 >= max_retries THEN 'failed'
				ELSE 'pending'
			END,
			retry_count = retry_count + 1,
			last_error = $1,
			processed_at = NULL
		WHERE id = $2
	`

	_, err := r.db.pool.Exec(ctx, sql, errMsg, id)
	if err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}

	return nil
}

// GetEventByID retrieves an event by its ID.
func (r *OutboxRepository) GetEventByID(ctx context.Context, eventID string) (*EventRecord, error) {
	sql := `
		SELECT event_id, chain, block_number, block_hash, block_timestamp,
		       tx_hash, tx_index, event_index, event_type, program_id,
		       accounts, payload, commitment_level, reorg_action,
		       replaces_event_id, native_value, schema_version, ingested_at, created_at
		FROM events
		WHERE event_id = $1
	`

	var event EventRecord
	err := r.db.pool.QueryRow(ctx, sql, eventID).Scan(
		&event.EventID, &event.Chain, &event.BlockNumber, &event.BlockHash, &event.BlockTimestamp,
		&event.TxHash, &event.TxIndex, &event.EventIndex, &event.EventType, &event.ProgramID,
		&event.Accounts, &event.Payload, &event.CommitmentLevel, &event.ReorgAction,
		&event.ReplacesEventID, &event.NativeValue, &event.SchemaVersion, &event.IngestedAt, &event.CreatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query event: %w", err)
	}

	return &event, nil
}

// GetEventsByBlock retrieves all events for a specific block.
func (r *OutboxRepository) GetEventsByBlock(ctx context.Context, chain int16, blockNumber int64) ([]EventRecord, error) {
	sql := `
		SELECT event_id, chain, block_number, block_hash, block_timestamp,
		       tx_hash, tx_index, event_index, event_type, program_id,
		       accounts, payload, commitment_level, reorg_action,
		       replaces_event_id, native_value, schema_version, ingested_at, created_at
		FROM events
		WHERE chain = $1 AND block_number = $2
		ORDER BY tx_index, event_index
	`

	rows, err := r.db.pool.Query(ctx, sql, chain, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	var events []EventRecord
	for rows.Next() {
		var event EventRecord
		err := rows.Scan(
			&event.EventID, &event.Chain, &event.BlockNumber, &event.BlockHash, &event.BlockTimestamp,
			&event.TxHash, &event.TxIndex, &event.EventIndex, &event.EventType, &event.ProgramID,
			&event.Accounts, &event.Payload, &event.CommitmentLevel, &event.ReorgAction,
			&event.ReplacesEventID, &event.NativeValue, &event.SchemaVersion, &event.IngestedAt, &event.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		events = append(events, event)
	}

	return events, rows.Err()
}
