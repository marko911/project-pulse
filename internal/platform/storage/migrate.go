package storage

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type MigrationRecord struct {
	Version   int
	Name      string
	AppliedAt time.Time
}

func (db *DB) Migrate(ctx context.Context) error {
	if err := db.ensureMigrationsTable(ctx); err != nil {
		return fmt.Errorf("ensure migrations table: %w", err)
	}

	applied, err := db.getAppliedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("get applied migrations: %w", err)
	}

	pending, err := db.getPendingMigrations(applied)
	if err != nil {
		return fmt.Errorf("get pending migrations: %w", err)
	}

	for _, mig := range pending {
		if err := db.applyMigration(ctx, mig); err != nil {
			return fmt.Errorf("apply migration %s: %w", mig.name, err)
		}
	}

	return nil
}

func (db *DB) MigrateDown(ctx context.Context, steps int) error {
	applied, err := db.getAppliedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("get applied migrations: %w", err)
	}

	if len(applied) == 0 {
		return nil
	}

	sort.Slice(applied, func(i, j int) bool {
		return applied[i].Version > applied[j].Version
	})

	if steps > len(applied) {
		steps = len(applied)
	}

	for i := 0; i < steps; i++ {
		mig := applied[i]
		if err := db.rollbackMigration(ctx, mig.Version, mig.Name); err != nil {
			return fmt.Errorf("rollback migration %s: %w", mig.Name, err)
		}
	}

	return nil
}

type migration struct {
	version int
	name    string
	sql     string
}

func (db *DB) ensureMigrationsTable(ctx context.Context) error {
	sql := `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`
	_, err := db.pool.Exec(ctx, sql)
	return err
}

func (db *DB) getAppliedMigrations(ctx context.Context) ([]MigrationRecord, error) {
	sql := `SELECT version, name, applied_at FROM schema_migrations ORDER BY version`
	rows, err := db.pool.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []MigrationRecord
	for rows.Next() {
		var r MigrationRecord
		if err := rows.Scan(&r.Version, &r.Name, &r.AppliedAt); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

func (db *DB) getPendingMigrations(applied []MigrationRecord) ([]migration, error) {
	appliedSet := make(map[int]bool)
	for _, a := range applied {
		appliedSet[a.Version] = true
	}

	var migrations []migration

	err := fs.WalkDir(migrationsFS, "migrations", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || !strings.HasSuffix(path, ".up.sql") {
			return nil
		}

		base := filepath.Base(path)
		parts := strings.SplitN(base, "_", 2)
		if len(parts) < 2 {
			return nil
		}

		var version int
		if _, err := fmt.Sscanf(parts[0], "%d", &version); err != nil {
			return nil
		}

		if appliedSet[version] {
			return nil
		}

		content, err := fs.ReadFile(migrationsFS, path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}

		name := strings.TrimSuffix(base, ".up.sql")
		migrations = append(migrations, migration{
			version: version,
			name:    name,
			sql:     string(content),
		})

		return nil
	})

	if err != nil {
		return nil, err
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].version < migrations[j].version
	})

	return migrations, nil
}

func (db *DB) applyMigration(ctx context.Context, mig migration) error {
	return db.WithTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, mig.sql); err != nil {
			return fmt.Errorf("execute sql: %w", err)
		}

		recordSQL := `INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`
		if _, err := tx.Exec(ctx, recordSQL, mig.version, mig.name); err != nil {
			return fmt.Errorf("record migration: %w", err)
		}

		return nil
	})
}

func (db *DB) rollbackMigration(ctx context.Context, version int, name string) error {
	downFile := fmt.Sprintf("migrations/%s.down.sql", name)

	content, err := fs.ReadFile(migrationsFS, downFile)
	if err != nil {
		return fmt.Errorf("read down migration: %w", err)
	}

	return db.WithTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, string(content)); err != nil {
			return fmt.Errorf("execute rollback: %w", err)
		}

		deleteSQL := `DELETE FROM schema_migrations WHERE version = $1`
		if _, err := tx.Exec(ctx, deleteSQL, version); err != nil {
			return fmt.Errorf("delete record: %w", err)
		}

		return nil
	})
}
