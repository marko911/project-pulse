package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

// ManifestRecord represents a manifest stored in the database.
type ManifestRecord struct {
	ID                 int64     `db:"id"`
	Chain              int16     `db:"chain"`
	BlockNumber        int64     `db:"block_number"`
	BlockHash          string    `db:"block_hash"`
	ParentHash         string    `db:"parent_hash"`
	ExpectedTxCount    int32     `db:"expected_tx_count"`
	ExpectedEventCount int32     `db:"expected_event_count"`
	EmittedTxCount     int32     `db:"emitted_tx_count"`
	EmittedEventCount  int32     `db:"emitted_event_count"`
	EventIdsHash       string    `db:"event_ids_hash"`
	SourcesUsed        []byte    `db:"sources_used"` // JSONB
	BlockTimestamp     time.Time `db:"block_timestamp"`
	IngestedAt         time.Time `db:"ingested_at"`
	ManifestCreatedAt  time.Time `db:"manifest_created_at"`
}

// ManifestRepository handles persistence of manifests.
type ManifestRepository struct {
	db *DB
}

// NewManifestRepository creates a new ManifestRepository.
func NewManifestRepository(db *DB) *ManifestRepository {
	return &ManifestRepository{db: db}
}

// Save persists a manifest to the database.
// Uses upsert semantics - if a manifest for the same chain/block exists, it's replaced.
func (r *ManifestRepository) Save(ctx context.Context, manifest *protov1.Manifest) error {
	sourcesJSON, err := json.Marshal(manifest.SourcesUsed)
	if err != nil {
		return fmt.Errorf("marshal sources: %w", err)
	}

	sql := `
		INSERT INTO manifests (
			chain, block_number, block_hash, parent_hash,
			expected_tx_count, expected_event_count,
			emitted_tx_count, emitted_event_count,
			event_ids_hash, sources_used, block_timestamp,
			ingested_at, manifest_created_at
		) VALUES (
			$1, $2, $3, $4,
			$5, $6,
			$7, $8,
			$9, $10, $11,
			$12, $13
		)
		ON CONFLICT (chain, block_number, block_timestamp) DO UPDATE SET
			block_hash = EXCLUDED.block_hash,
			parent_hash = EXCLUDED.parent_hash,
			expected_tx_count = EXCLUDED.expected_tx_count,
			expected_event_count = EXCLUDED.expected_event_count,
			emitted_tx_count = EXCLUDED.emitted_tx_count,
			emitted_event_count = EXCLUDED.emitted_event_count,
			event_ids_hash = EXCLUDED.event_ids_hash,
			sources_used = EXCLUDED.sources_used,
			ingested_at = EXCLUDED.ingested_at,
			manifest_created_at = EXCLUDED.manifest_created_at
	`

	_, err = r.db.pool.Exec(ctx, sql,
		int16(manifest.Chain),
		int64(manifest.BlockNumber),
		manifest.BlockHash,
		manifest.ParentHash,
		int32(manifest.ExpectedTxCount),
		int32(manifest.ExpectedEventCount),
		int32(manifest.EmittedTxCount),
		int32(manifest.EmittedEventCount),
		manifest.EventIdsHash,
		sourcesJSON,
		manifest.BlockTimestamp,
		manifest.IngestedAt,
		manifest.ManifestCreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert manifest: %w", err)
	}

	return nil
}

// SaveBatch persists multiple manifests in a single transaction.
func (r *ManifestRepository) SaveBatch(ctx context.Context, manifests []*protov1.Manifest) error {
	if len(manifests) == 0 {
		return nil
	}

	return r.db.WithTx(ctx, func(tx pgx.Tx) error {
		for _, manifest := range manifests {
			sourcesJSON, err := json.Marshal(manifest.SourcesUsed)
			if err != nil {
				return fmt.Errorf("marshal sources for block %d: %w", manifest.BlockNumber, err)
			}

			sql := `
				INSERT INTO manifests (
					chain, block_number, block_hash, parent_hash,
					expected_tx_count, expected_event_count,
					emitted_tx_count, emitted_event_count,
					event_ids_hash, sources_used, block_timestamp,
					ingested_at, manifest_created_at
				) VALUES (
					$1, $2, $3, $4,
					$5, $6,
					$7, $8,
					$9, $10, $11,
					$12, $13
				)
				ON CONFLICT (chain, block_number, block_timestamp) DO UPDATE SET
					block_hash = EXCLUDED.block_hash,
					parent_hash = EXCLUDED.parent_hash,
					expected_tx_count = EXCLUDED.expected_tx_count,
					expected_event_count = EXCLUDED.expected_event_count,
					emitted_tx_count = EXCLUDED.emitted_tx_count,
					emitted_event_count = EXCLUDED.emitted_event_count,
					event_ids_hash = EXCLUDED.event_ids_hash,
					sources_used = EXCLUDED.sources_used,
					ingested_at = EXCLUDED.ingested_at,
					manifest_created_at = EXCLUDED.manifest_created_at
			`

			_, err = tx.Exec(ctx, sql,
				int16(manifest.Chain),
				int64(manifest.BlockNumber),
				manifest.BlockHash,
				manifest.ParentHash,
				int32(manifest.ExpectedTxCount),
				int32(manifest.ExpectedEventCount),
				int32(manifest.EmittedTxCount),
				int32(manifest.EmittedEventCount),
				manifest.EventIdsHash,
				sourcesJSON,
				manifest.BlockTimestamp,
				manifest.IngestedAt,
				manifest.ManifestCreatedAt,
			)
			if err != nil {
				return fmt.Errorf("insert manifest for block %d: %w", manifest.BlockNumber, err)
			}
		}
		return nil
	})
}

// GetByBlock retrieves a manifest by chain and block number.
func (r *ManifestRepository) GetByBlock(ctx context.Context, chain protov1.Chain, blockNumber uint64) (*ManifestRecord, error) {
	sql := `
		SELECT id, chain, block_number, block_hash, parent_hash,
		       expected_tx_count, expected_event_count,
		       emitted_tx_count, emitted_event_count,
		       event_ids_hash, sources_used, block_timestamp,
		       ingested_at, manifest_created_at
		FROM manifests
		WHERE chain = $1 AND block_number = $2
		ORDER BY manifest_created_at DESC
		LIMIT 1
	`

	var m ManifestRecord
	err := r.db.pool.QueryRow(ctx, sql, int16(chain), int64(blockNumber)).Scan(
		&m.ID, &m.Chain, &m.BlockNumber, &m.BlockHash, &m.ParentHash,
		&m.ExpectedTxCount, &m.ExpectedEventCount,
		&m.EmittedTxCount, &m.EmittedEventCount,
		&m.EventIdsHash, &m.SourcesUsed, &m.BlockTimestamp,
		&m.IngestedAt, &m.ManifestCreatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query manifest: %w", err)
	}

	return &m, nil
}

// GetByBlockHash retrieves a manifest by chain and block hash.
func (r *ManifestRepository) GetByBlockHash(ctx context.Context, chain protov1.Chain, blockHash string) (*ManifestRecord, error) {
	sql := `
		SELECT id, chain, block_number, block_hash, parent_hash,
		       expected_tx_count, expected_event_count,
		       emitted_tx_count, emitted_event_count,
		       event_ids_hash, sources_used, block_timestamp,
		       ingested_at, manifest_created_at
		FROM manifests
		WHERE chain = $1 AND block_hash = $2
		LIMIT 1
	`

	var m ManifestRecord
	err := r.db.pool.QueryRow(ctx, sql, int16(chain), blockHash).Scan(
		&m.ID, &m.Chain, &m.BlockNumber, &m.BlockHash, &m.ParentHash,
		&m.ExpectedTxCount, &m.ExpectedEventCount,
		&m.EmittedTxCount, &m.EmittedEventCount,
		&m.EventIdsHash, &m.SourcesUsed, &m.BlockTimestamp,
		&m.IngestedAt, &m.ManifestCreatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query manifest by hash: %w", err)
	}

	return &m, nil
}

// GetBlockRange retrieves manifests for a range of blocks.
func (r *ManifestRepository) GetBlockRange(ctx context.Context, chain protov1.Chain, fromBlock, toBlock uint64) ([]ManifestRecord, error) {
	sql := `
		SELECT id, chain, block_number, block_hash, parent_hash,
		       expected_tx_count, expected_event_count,
		       emitted_tx_count, emitted_event_count,
		       event_ids_hash, sources_used, block_timestamp,
		       ingested_at, manifest_created_at
		FROM manifests
		WHERE chain = $1 AND block_number >= $2 AND block_number <= $3
		ORDER BY block_number ASC
	`

	rows, err := r.db.pool.Query(ctx, sql, int16(chain), int64(fromBlock), int64(toBlock))
	if err != nil {
		return nil, fmt.Errorf("query manifests: %w", err)
	}
	defer rows.Close()

	var manifests []ManifestRecord
	for rows.Next() {
		var m ManifestRecord
		err := rows.Scan(
			&m.ID, &m.Chain, &m.BlockNumber, &m.BlockHash, &m.ParentHash,
			&m.ExpectedTxCount, &m.ExpectedEventCount,
			&m.EmittedTxCount, &m.EmittedEventCount,
			&m.EventIdsHash, &m.SourcesUsed, &m.BlockTimestamp,
			&m.IngestedAt, &m.ManifestCreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		manifests = append(manifests, m)
	}

	return manifests, rows.Err()
}

// FindMismatches returns manifests where expected counts don't match emitted counts.
func (r *ManifestRepository) FindMismatches(ctx context.Context, chain protov1.Chain, limit int) ([]ManifestRecord, error) {
	sql := `
		SELECT id, chain, block_number, block_hash, parent_hash,
		       expected_tx_count, expected_event_count,
		       emitted_tx_count, emitted_event_count,
		       event_ids_hash, sources_used, block_timestamp,
		       ingested_at, manifest_created_at
		FROM manifests
		WHERE chain = $1
		  AND (expected_tx_count != emitted_tx_count
		       OR expected_event_count != emitted_event_count)
		ORDER BY block_number DESC
		LIMIT $2
	`

	rows, err := r.db.pool.Query(ctx, sql, int16(chain), limit)
	if err != nil {
		return nil, fmt.Errorf("query mismatches: %w", err)
	}
	defer rows.Close()

	var manifests []ManifestRecord
	for rows.Next() {
		var m ManifestRecord
		err := rows.Scan(
			&m.ID, &m.Chain, &m.BlockNumber, &m.BlockHash, &m.ParentHash,
			&m.ExpectedTxCount, &m.ExpectedEventCount,
			&m.EmittedTxCount, &m.EmittedEventCount,
			&m.EventIdsHash, &m.SourcesUsed, &m.BlockTimestamp,
			&m.IngestedAt, &m.ManifestCreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		manifests = append(manifests, m)
	}

	return manifests, rows.Err()
}

// GetLatestBlock returns the highest block number with a manifest for the given chain.
func (r *ManifestRepository) GetLatestBlock(ctx context.Context, chain protov1.Chain) (uint64, error) {
	sql := `
		SELECT COALESCE(MAX(block_number), 0)
		FROM manifests
		WHERE chain = $1
	`

	var blockNumber int64
	err := r.db.pool.QueryRow(ctx, sql, int16(chain)).Scan(&blockNumber)
	if err != nil {
		return 0, fmt.Errorf("query latest block: %w", err)
	}

	return uint64(blockNumber), nil
}

// CountByChain returns the total number of manifests for a chain.
func (r *ManifestRepository) CountByChain(ctx context.Context, chain protov1.Chain) (int64, error) {
	sql := `SELECT COUNT(*) FROM manifests WHERE chain = $1`

	var count int64
	err := r.db.pool.QueryRow(ctx, sql, int16(chain)).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count manifests: %w", err)
	}

	return count, nil
}

// ToProto converts a ManifestRecord to a protov1.Manifest.
func (m *ManifestRecord) ToProto() (*protov1.Manifest, error) {
	var sources []string
	if err := json.Unmarshal(m.SourcesUsed, &sources); err != nil {
		return nil, fmt.Errorf("unmarshal sources: %w", err)
	}

	return &protov1.Manifest{
		Chain:              protov1.Chain(m.Chain),
		BlockNumber:        uint64(m.BlockNumber),
		BlockHash:          m.BlockHash,
		ParentHash:         m.ParentHash,
		ExpectedTxCount:    uint32(m.ExpectedTxCount),
		ExpectedEventCount: uint32(m.ExpectedEventCount),
		EmittedTxCount:     uint32(m.EmittedTxCount),
		EmittedEventCount:  uint32(m.EmittedEventCount),
		EventIdsHash:       m.EventIdsHash,
		SourcesUsed:        sources,
		BlockTimestamp:     m.BlockTimestamp,
		IngestedAt:         m.IngestedAt,
		ManifestCreatedAt:  m.ManifestCreatedAt,
	}, nil
}
