package goldensource

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	protov1 "github.com/marko911/project-pulse/pkg/proto/v1"
)

type VerificationCallback func(ctx context.Context, result *VerificationResult) error

type Verifier struct {
	cfg     VerifierConfig
	logger  *slog.Logger
	clients map[protov1.Chain]Client

	mu         sync.RWMutex
	callbacks  []VerificationCallback
	halted     bool
	haltReason string
	stats      *VerifierStats
}

type VerifierConfig struct {
	FailClosed bool

	VerifyEveryNthBlock uint64

	SkipIfGoldenSourceUnavailable bool
}

func DefaultVerifierConfig() VerifierConfig {
	return VerifierConfig{
		FailClosed:                    true,
		VerifyEveryNthBlock:           1,
		SkipIfGoldenSourceUnavailable: false,
	}
}

type VerifierStats struct {
	BlocksVerified     uint64
	BlocksPassed       uint64
	BlocksFailed       uint64
	BlocksSkipped      uint64
	GoldenSourceErrors uint64
	LastVerifiedBlock  uint64
	LastVerifiedAt     time.Time
}

func NewVerifier(cfg VerifierConfig, logger *slog.Logger) *Verifier {
	if logger == nil {
		logger = slog.Default()
	}

	return &Verifier{
		cfg:     cfg,
		logger:  logger.With("component", "golden-verifier"),
		clients: make(map[protov1.Chain]Client),
		stats:   &VerifierStats{},
	}
}

func (v *Verifier) RegisterClient(client Client) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.clients[client.Chain()] = client
	v.logger.Info("registered golden source client",
		"chain", client.Chain(),
		"name", client.Name(),
	)
}

func (v *Verifier) OnVerification(cb VerificationCallback) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.callbacks = append(v.callbacks, cb)
}

func (v *Verifier) VerifyBlock(ctx context.Context, primary *BlockData) (*VerificationResult, error) {
	v.mu.Lock()
	if v.halted && v.cfg.FailClosed {
		v.mu.Unlock()
		return nil, fmt.Errorf("verifier halted: %s", v.haltReason)
	}

	if v.cfg.VerifyEveryNthBlock > 1 && primary.BlockNumber%v.cfg.VerifyEveryNthBlock != 0 {
		v.stats.BlocksSkipped++
		v.mu.Unlock()
		return nil, nil
	}

	client, exists := v.clients[primary.Chain]
	v.mu.Unlock()

	if !exists {
		if v.cfg.SkipIfGoldenSourceUnavailable {
			v.mu.Lock()
			v.stats.BlocksSkipped++
			v.mu.Unlock()
			return nil, nil
		}
		return nil, fmt.Errorf("no golden source client for chain %v", primary.Chain)
	}

	result, err := client.VerifyBlock(ctx, primary)
	if err != nil {
		v.mu.Lock()
		v.stats.GoldenSourceErrors++
		v.mu.Unlock()

		if errors.Is(err, ErrNotConnected) || errors.Is(err, ErrBlockNotFound) {
			if v.cfg.SkipIfGoldenSourceUnavailable {
				v.mu.Lock()
				v.stats.BlocksSkipped++
				v.mu.Unlock()
				v.logger.Warn("skipping verification due to golden source unavailability",
					"block", primary.BlockNumber,
					"error", err,
				)
				return nil, nil
			}
		}
		return nil, err
	}

	v.mu.Lock()
	v.stats.BlocksVerified++
	v.stats.LastVerifiedBlock = primary.BlockNumber
	v.stats.LastVerifiedAt = time.Now()

	if result.Verified {
		v.stats.BlocksPassed++
	} else {
		v.stats.BlocksFailed++

		if v.cfg.FailClosed {
			v.halted = true
			v.haltReason = fmt.Sprintf("verification failed for block %d: %v",
				primary.BlockNumber, result.Errors)
		}
	}

	callbacks := make([]VerificationCallback, len(v.callbacks))
	copy(callbacks, v.callbacks)
	v.mu.Unlock()

	for _, cb := range callbacks {
		if err := cb(ctx, result); err != nil {
			v.logger.Error("verification callback failed", "error", err)
		}
	}

	if !result.Verified && v.cfg.FailClosed {
		return result, fmt.Errorf("fail-closed: %s", v.haltReason)
	}

	return result, nil
}

func (v *Verifier) ResolveFailure(reason string) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if !v.halted {
		return fmt.Errorf("verifier is not halted")
	}

	v.logger.Info("verification failure resolved",
		"previous_reason", v.haltReason,
		"resolution", reason,
	)

	v.halted = false
	v.haltReason = ""
	return nil
}

func (v *Verifier) IsHalted() bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.halted
}

func (v *Verifier) HaltReason() string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.haltReason
}

func (v *Verifier) Stats() VerifierStats {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return *v.stats
}

func (v *Verifier) ConnectAll(ctx context.Context) error {
	v.mu.RLock()
	clients := make([]Client, 0, len(v.clients))
	for _, c := range v.clients {
		clients = append(clients, c)
	}
	v.mu.RUnlock()

	for _, client := range clients {
		if err := client.Connect(ctx); err != nil {
			return fmt.Errorf("failed to connect %s: %w", client.Name(), err)
		}
	}
	return nil
}

func (v *Verifier) CloseAll() error {
	v.mu.RLock()
	clients := make([]Client, 0, len(v.clients))
	for _, c := range v.clients {
		clients = append(clients, c)
	}
	v.mu.RUnlock()

	var errs []error
	for _, client := range clients {
		if err := client.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing clients: %v", errs)
	}
	return nil
}
