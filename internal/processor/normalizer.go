package processor

import (
	"context"
	"fmt"

	"github.com/marko911/project-pulse/internal/adapter"
	eventpb "github.com/marko911/project-pulse/pkg/proto/v1"
)

type NormalizerRegistry struct {
	normalizers map[string]Normalizer
}

func NewNormalizerRegistry() *NormalizerRegistry {
	reg := &NormalizerRegistry{
		normalizers: make(map[string]Normalizer),
	}

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

func (r *NormalizerRegistry) Register(n Normalizer) {
	r.normalizers[n.Chain()] = n
}

func (r *NormalizerRegistry) Get(chain string) (Normalizer, bool) {
	n, ok := r.normalizers[chain]
	return n, ok
}

func (r *NormalizerRegistry) Normalize(ctx context.Context, event adapter.Event) (*eventpb.CanonicalEvent, error) {
	n, ok := r.Get(event.Chain)
	if !ok {
		return nil, fmt.Errorf("no normalizer registered for chain: %s", event.Chain)
	}
	return n.Normalize(ctx, event)
}
