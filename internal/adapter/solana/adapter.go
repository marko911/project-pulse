// Package solana implements the Solana blockchain adapter using Yellowstone gRPC.
package solana

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mirador/pulse/internal/adapter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

// Adapter implements the Solana blockchain adapter.
type Adapter struct {
	cfg    Config
	logger *slog.Logger

	mu      sync.RWMutex
	running bool
	cancel  context.CancelFunc
	conn    *grpc.ClientConn
}

// Config holds Solana-specific adapter configuration.
type Config struct {
	adapter.Config

	// Yellowstone/Geyser gRPC endpoint
	GeyserEndpoint string

	// X-Token for Geyser authentication (if required)
	GeyserToken string

	// Use TLS for connection
	UseTLS bool

	// Subscribe to specific program accounts
	ProgramSubscriptions []string

	// Subscribe to all transactions
	SubscribeTransactions bool

	// Subscribe to account updates
	SubscribeAccounts bool
}

// New creates a new Solana adapter with the given configuration.
func New(cfg Config, logger *slog.Logger) *Adapter {
	if logger == nil {
		logger = slog.Default()
	}
	return &Adapter{
		cfg:    cfg,
		logger: logger.With("adapter", "solana"),
	}
}

// Name returns the adapter identifier.
func (a *Adapter) Name() string {
	return "solana"
}

// Start begins streaming events from Solana via Yellowstone/Geyser gRPC.
func (a *Adapter) Start(ctx context.Context, events chan<- adapter.Event) error {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return fmt.Errorf("adapter already running")
	}

	ctx, cancel := context.WithCancel(ctx)
	a.cancel = cancel
	a.running = true
	a.mu.Unlock()

	a.logger.Info("starting solana adapter",
		"geyser_endpoint", a.cfg.GeyserEndpoint,
		"commitment", a.cfg.CommitmentLevel,
	)

	// Connect to Geyser gRPC
	if err := a.connect(ctx); err != nil {
		a.mu.Lock()
		a.running = false
		a.mu.Unlock()
		return fmt.Errorf("failed to connect to geyser: %w", err)
	}

	go a.streamEvents(ctx, events)

	return nil
}

// connect establishes the gRPC connection to Geyser.
func (a *Adapter) connect(ctx context.Context) error {
	opts := []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(64 * 1024 * 1024), // 64MB for large blocks
		),
	}

	// TLS configuration
	if a.cfg.UseTLS {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
		})))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.DialContext(ctx, a.cfg.GeyserEndpoint, opts...)
	if err != nil {
		return fmt.Errorf("grpc dial failed: %w", err)
	}

	a.mu.Lock()
	a.conn = conn
	a.mu.Unlock()

	a.logger.Info("connected to geyser", "endpoint", a.cfg.GeyserEndpoint)
	return nil
}

// streamEvents handles the main event streaming loop.
func (a *Adapter) streamEvents(ctx context.Context, events chan<- adapter.Event) {
	defer func() {
		a.mu.Lock()
		a.running = false
		if a.conn != nil {
			a.conn.Close()
			a.conn = nil
		}
		a.mu.Unlock()
		a.logger.Info("solana adapter stopped")
	}()

	// Add auth token to context if provided
	if a.cfg.GeyserToken != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-token", a.cfg.GeyserToken)
	}

	// Reconnection loop with exponential backoff
	backoff := time.Second
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := a.subscribeAndStream(ctx, events)
		if err != nil {
			if ctx.Err() != nil {
				return // Context cancelled, exit gracefully
			}

			a.logger.Error("stream error, reconnecting",
				"error", err,
				"backoff", backoff,
			)

			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}

			// Exponential backoff
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		} else {
			// Reset backoff on successful stream
			backoff = time.Second
		}
	}
}

// subscribeAndStream handles the Geyser subscription and event streaming.
func (a *Adapter) subscribeAndStream(ctx context.Context, events chan<- adapter.Event) error {
	a.mu.RLock()
	conn := a.conn
	a.mu.RUnlock()

	if conn == nil {
		if err := a.connect(ctx); err != nil {
			return err
		}
	}

	// Create Geyser client and subscribe
	// Note: Using the Yellowstone gRPC proto definitions
	// The actual proto would come from github.com/rpcpool/yellowstone-grpc
	//
	// For now, we implement a simulation of the subscription flow.
	// In production, this would use the generated Yellowstone gRPC client.

	a.logger.Info("subscribing to geyser stream",
		"transactions", a.cfg.SubscribeTransactions,
		"accounts", a.cfg.SubscribeAccounts,
		"programs", a.cfg.ProgramSubscriptions,
	)

	// Simulated event streaming - in production this would be:
	// client := yellowstone.NewGeyserClient(conn)
	// stream, err := client.Subscribe(ctx)
	// ...send subscription request...
	// for { msg, err := stream.Recv(); ... }

	// For the scaffold, we wait for context cancellation
	// Real implementation will use the Yellowstone gRPC Subscribe stream
	<-ctx.Done()
	return ctx.Err()
}

// parseTransaction converts a Geyser transaction update to an adapter Event.
func (a *Adapter) parseTransaction(slot uint64, signature []byte, accounts [][]byte, data []byte) adapter.Event {
	// Convert signature to base58 string
	sig := base64.StdEncoding.EncodeToString(signature)

	accountStrs := make([]string, len(accounts))
	for i, acc := range accounts {
		accountStrs[i] = base64.StdEncoding.EncodeToString(acc)
	}

	return adapter.Event{
		Chain:           "solana",
		CommitmentLevel: a.cfg.CommitmentLevel,
		BlockNumber:     slot,
		TxHash:          sig,
		EventType:       "transaction",
		Accounts:        accountStrs,
		Timestamp:       time.Now().Unix(),
		Payload:         data,
	}
}

// parseAccountUpdate converts a Geyser account update to an adapter Event.
func (a *Adapter) parseAccountUpdate(slot uint64, pubkey []byte, owner []byte, data []byte) adapter.Event {
	return adapter.Event{
		Chain:           "solana",
		CommitmentLevel: a.cfg.CommitmentLevel,
		BlockNumber:     slot,
		EventType:       "account_update",
		Accounts:        []string{base64.StdEncoding.EncodeToString(pubkey)},
		ProgramID:       base64.StdEncoding.EncodeToString(owner),
		Timestamp:       time.Now().Unix(),
		Payload:         data,
	}
}

// Stop gracefully shuts down the adapter.
func (a *Adapter) Stop(ctx context.Context) error {
	a.mu.RLock()
	if !a.running {
		a.mu.RUnlock()
		return nil
	}
	cancel := a.cancel
	a.mu.RUnlock()

	if cancel != nil {
		cancel()
	}

	a.logger.Info("stopping solana adapter")
	return nil
}

// Health returns the current health status of the adapter.
func (a *Adapter) Health(ctx context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if !a.running {
		return fmt.Errorf("adapter not running")
	}

	if a.conn == nil {
		return fmt.Errorf("no gRPC connection")
	}

	return nil
}
