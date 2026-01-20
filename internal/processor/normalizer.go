package processor

import (
	"context"
	"fmt"

	"github.com/marko911/project-pulse/internal/adapter"
	eventpb "github.com/marko911/project-pulse/pkg/proto/v1"
)

// NormalizerRegistry holds normalizers for each supported chain.
type NormalizerRegistry struct {
	normalizers map[string]Normalizer
}

// NewNormalizerRegistry creates a new registry with default normalizers.
func NewNormalizerRegistry() *NormalizerRegistry {
	reg := &NormalizerRegistry{
		normalizers: make(map[string]Normalizer),
	}

	// Register default normalizers
	reg.Register(NewSolanaNormalizer())
	reg.Register(NewEVMNormalizer("ethereum"))
	reg.Register(NewEVMNormalizer("polygon"))
	reg.Register(NewEVMNormalizer("arbitrum"))
	reg.Register(NewEVMNormalizer("optimism"))
	reg.Register(NewEVMNormalizer("base"))
	reg.Register(NewEVMNormalizer("avalanche"))
	reg.Register(NewEVMNormalizer("bsc"))

	return reg
}

// Register adds a normalizer to the registry.
func (r *NormalizerRegistry) Register(n Normalizer) {
	r.normalizers[n.Chain()] = n
}

// Get retrieves a normalizer for the given chain.
func (r *NormalizerRegistry) Get(chain string) (Normalizer, bool) {
	n, ok := r.normalizers[chain]
	return n, ok
}

// Normalize converts an adapter event to canonical format using the appropriate normalizer.
func (r *NormalizerRegistry) Normalize(ctx context.Context, event adapter.Event) (*eventpb.CanonicalEvent, error) {
	n, ok := r.Get(event.Chain)
	if !ok {
		return nil, fmt.Errorf("no normalizer registered for chain: %s", event.Chain)
	}
	return n.Normalize(ctx, event)
}
