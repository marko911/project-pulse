package storage

import (
	"context"
	"embed"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string

	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
}

func DefaultConfig() Config {
	return Config{
		Host:              "localhost",
		Port:              5432,
		User:              "pulse",
		Password:          "pulse_dev",
		Database:          "pulse",
		SSLMode:           "disable",
		MaxConns:          25,
		MinConns:          5,
		MaxConnLifetime:   time.Hour,
		MaxConnIdleTime:   30 * time.Minute,
		HealthCheckPeriod: time.Minute,
	}
}

func (c Config) ConnectionString() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.Database, c.SSLMode,
	)
}

type DB struct {
	pool *pgxpool.Pool
	cfg  Config
}

func New(ctx context.Context, cfg Config) (*DB, error) {
	defaults := DefaultConfig()
	if cfg.MaxConns == 0 {
		cfg.MaxConns = defaults.MaxConns
	}
	if cfg.MinConns == 0 {
		cfg.MinConns = defaults.MinConns
	}
	if cfg.MaxConnLifetime == 0 {
		cfg.MaxConnLifetime = defaults.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime == 0 {
		cfg.MaxConnIdleTime = defaults.MaxConnIdleTime
	}
	if cfg.HealthCheckPeriod == 0 {
		cfg.HealthCheckPeriod = defaults.HealthCheckPeriod
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.ConnectionString())
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	poolCfg.HealthCheckPeriod = cfg.HealthCheckPeriod

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}

	return &DB{pool: pool, cfg: cfg}, nil
}

func (db *DB) Close() {
	db.pool.Close()
}

func (db *DB) Pool() *pgxpool.Pool {
	return db.pool
}

func (db *DB) Health(ctx context.Context) error {
	return db.pool.Ping(ctx)
}

func (db *DB) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return db.pool.Begin(ctx)
}

func (db *DB) WithTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p)
		}
	}()

	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return fmt.Errorf("rollback failed: %v (original: %w)", rbErr, err)
		}
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}
