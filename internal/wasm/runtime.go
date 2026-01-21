package wasm

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/bytecodealliance/wasmtime-go/v30"
)

type RuntimeConfig struct {
	MaxMemoryMB int
	MaxCPUMs    int
	CacheSize   int
}

type ExecutionResult struct {
	Output      []byte
	DurationMs  int64
	MemoryBytes int64
}

type CompiledModule struct {
	Module     *wasmtime.Module
	CompiledAt time.Time
}

type Runtime struct {
	cfg    RuntimeConfig
	engine *wasmtime.Engine
	logger *slog.Logger

	cacheMu sync.RWMutex
	cache   map[string]*CompiledModule
}

func NewRuntime(cfg RuntimeConfig, logger *slog.Logger) (*Runtime, error) {
	engineCfg := wasmtime.NewConfig()
	engineCfg.SetEpochInterruption(true)
	engineCfg.SetConsumeFuel(false)

	engine := wasmtime.NewEngineWithConfig(engineCfg)

	return &Runtime{
		cfg:    cfg,
		engine: engine,
		logger: logger,
		cache:  make(map[string]*CompiledModule),
	}, nil
}

func (r *Runtime) Compile(moduleID string, wasmBytes []byte) (*CompiledModule, error) {
	r.cacheMu.RLock()
	if cached, ok := r.cache[moduleID]; ok {
		r.cacheMu.RUnlock()
		return cached, nil
	}
	r.cacheMu.RUnlock()

	module, err := wasmtime.NewModule(r.engine, wasmBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to compile module: %w", err)
	}

	compiled := &CompiledModule{
		Module:     module,
		CompiledAt: time.Now(),
	}

	r.cacheMu.Lock()
	if len(r.cache) >= r.cfg.CacheSize {
		var oldestKey string
		var oldestTime time.Time
		for k, v := range r.cache {
			if oldestKey == "" || v.CompiledAt.Before(oldestTime) {
				oldestKey = k
				oldestTime = v.CompiledAt
			}
		}
		delete(r.cache, oldestKey)
	}
	r.cache[moduleID] = compiled
	r.cacheMu.Unlock()

	return compiled, nil
}

func (r *Runtime) Execute(ctx context.Context, module *CompiledModule, input []byte, hostFuncs *HostFunctions) (*ExecutionResult, error) {
	startTime := time.Now()

	store := wasmtime.NewStore(r.engine)
	defer store.Close()

	store.Limiter(
		int64(r.cfg.MaxMemoryMB*1024*1024),
		-1,
		1,
		1,
		1,
	)

	store.SetEpochDeadline(1)

	hostFuncs.SetInput(input)

	linker := wasmtime.NewLinker(r.engine)

	wasiConfig := wasmtime.NewWasiConfig()
	wasiConfig.InheritEnv()
	store.SetWasi(wasiConfig)
	if err := linker.DefineWasi(); err != nil {
		return nil, fmt.Errorf("failed to define wasi: %w", err)
	}

	if err := r.addHostFunctions(ctx, linker, store, hostFuncs); err != nil {
		return nil, fmt.Errorf("failed to add host functions: %w", err)
	}

	instance, err := linker.Instantiate(store, module.Module)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate module: %w", err)
	}

	mainFunc := instance.GetFunc(store, "_start")
	if mainFunc == nil {
		mainFunc = instance.GetFunc(store, "main")
	}
	if mainFunc == nil {
		return nil, fmt.Errorf("no _start or main function found")
	}

	memory := instance.GetExport(store, "memory")

	done := make(chan struct{})
	go r.epochIncrementer(ctx, store, done)

	_, err = mainFunc.Call(store)
	close(done)

	duration := time.Since(startTime)

	if err != nil {
		if trap, ok := err.(*wasmtime.Trap); ok {
			if trap.Code() != nil && *trap.Code() == wasmtime.Interrupt {
				return nil, fmt.Errorf("execution timeout exceeded %dms", r.cfg.MaxCPUMs)
			}
		}
		errStr := err.Error()
		if strings.Contains(errStr, "exit status 0") {
		} else {
			return nil, fmt.Errorf("execution failed: %w", err)
		}
	}

	var memoryBytes int64
	if memory != nil && memory.Memory() != nil {
		memoryBytes = int64(memory.Memory().DataSize(store))
	}

	return &ExecutionResult{
		Output:      hostFuncs.GetOutput(),
		DurationMs:  duration.Milliseconds(),
		MemoryBytes: memoryBytes,
	}, nil
}

func (r *Runtime) epochIncrementer(ctx context.Context, store *wasmtime.Store, done <-chan struct{}) {
	tickInterval := time.Duration(r.cfg.MaxCPUMs) * time.Millisecond / 10
	if tickInterval < time.Millisecond {
		tickInterval = time.Millisecond
	}

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	epochCount := 0
	maxEpochs := 10

	for {
		select {
		case <-ticker.C:
			epochCount++
			r.engine.IncrementEpoch()
			if epochCount >= maxEpochs {
				return
			}
		case <-done:
			return
		case <-ctx.Done():
			for i := 0; i < maxEpochs; i++ {
				r.engine.IncrementEpoch()
			}
			return
		}
	}
}

func (r *Runtime) addHostFunctions(ctx context.Context, linker *wasmtime.Linker, store *wasmtime.Store, hostFuncs *HostFunctions) error {
	logFuncType := wasmtime.NewFuncType(
		[]*wasmtime.ValType{
			wasmtime.NewValType(wasmtime.KindI32),
			wasmtime.NewValType(wasmtime.KindI32),
			wasmtime.NewValType(wasmtime.KindI32),
		},
		[]*wasmtime.ValType{},
	)

	logFunc := wasmtime.NewFunc(store, logFuncType, func(caller *wasmtime.Caller, args []wasmtime.Val) ([]wasmtime.Val, *wasmtime.Trap) {
		level := args[0].I32()
		ptr := args[1].I32()
		length := args[2].I32()

		memory := caller.GetExport("memory")
		if memory == nil || memory.Memory() == nil {
			return nil, nil
		}

		data := memory.Memory().UnsafeData(caller)
		if int(ptr)+int(length) > len(data) {
			return nil, nil
		}

		msg := string(data[ptr : ptr+length])
		hostFuncs.Log(int(level), msg)
		return nil, nil
	})

	if err := linker.Define(store, "env", "log", logFunc); err != nil {
		return err
	}

	outputFuncType := wasmtime.NewFuncType(
		[]*wasmtime.ValType{
			wasmtime.NewValType(wasmtime.KindI32),
			wasmtime.NewValType(wasmtime.KindI32),
		},
		[]*wasmtime.ValType{},
	)

	outputFunc := wasmtime.NewFunc(store, outputFuncType, func(caller *wasmtime.Caller, args []wasmtime.Val) ([]wasmtime.Val, *wasmtime.Trap) {
		ptr := args[0].I32()
		length := args[1].I32()

		memory := caller.GetExport("memory")
		if memory == nil || memory.Memory() == nil {
			return nil, nil
		}

		data := memory.Memory().UnsafeData(caller)
		if int(ptr)+int(length) > len(data) {
			return nil, nil
		}

		hostFuncs.SetOutput(data[ptr : ptr+length])
		return nil, nil
	})

	if err := linker.Define(store, "env", "output", outputFunc); err != nil {
		return err
	}

	getInputLenFuncType := wasmtime.NewFuncType(
		[]*wasmtime.ValType{},
		[]*wasmtime.ValType{wasmtime.NewValType(wasmtime.KindI32)},
	)

	getInputLenFunc := wasmtime.NewFunc(store, getInputLenFuncType, func(caller *wasmtime.Caller, args []wasmtime.Val) ([]wasmtime.Val, *wasmtime.Trap) {
		return []wasmtime.Val{wasmtime.ValI32(int32(hostFuncs.GetInputLen()))}, nil
	})

	if err := linker.Define(store, "env", "get_input_len", getInputLenFunc); err != nil {
		return err
	}

	getInputFuncType := wasmtime.NewFuncType(
		[]*wasmtime.ValType{
			wasmtime.NewValType(wasmtime.KindI32),
			wasmtime.NewValType(wasmtime.KindI32),
		},
		[]*wasmtime.ValType{wasmtime.NewValType(wasmtime.KindI32)},
	)

	getInputFunc := wasmtime.NewFunc(store, getInputFuncType, func(caller *wasmtime.Caller, args []wasmtime.Val) ([]wasmtime.Val, *wasmtime.Trap) {
		ptr := args[0].I32()
		maxLen := args[1].I32()

		memory := caller.GetExport("memory")
		if memory == nil || memory.Memory() == nil {
			return []wasmtime.Val{wasmtime.ValI32(-1)}, nil
		}

		data := memory.Memory().UnsafeData(caller)
		if int(ptr)+int(maxLen) > len(data) {
			return []wasmtime.Val{wasmtime.ValI32(-1)}, nil
		}

		input := hostFuncs.GetInput()
		copyLen := len(input)
		if copyLen > int(maxLen) {
			copyLen = int(maxLen)
		}

		copy(data[ptr:ptr+int32(copyLen)], input[:copyLen])
		return []wasmtime.Val{wasmtime.ValI32(int32(copyLen))}, nil
	})

	if err := linker.Define(store, "env", "get_input", getInputFunc); err != nil {
		return err
	}

	kvGetFuncType := wasmtime.NewFuncType(
		[]*wasmtime.ValType{
			wasmtime.NewValType(wasmtime.KindI32),
			wasmtime.NewValType(wasmtime.KindI32),
			wasmtime.NewValType(wasmtime.KindI32),
			wasmtime.NewValType(wasmtime.KindI32),
		},
		[]*wasmtime.ValType{wasmtime.NewValType(wasmtime.KindI32)},
	)

	kvGetFunc := wasmtime.NewFunc(store, kvGetFuncType, func(caller *wasmtime.Caller, args []wasmtime.Val) ([]wasmtime.Val, *wasmtime.Trap) {
		keyPtr := args[0].I32()
		keyLen := args[1].I32()
		outPtr := args[2].I32()
		outLenPtr := args[3].I32()

		memory := caller.GetExport("memory")
		if memory == nil || memory.Memory() == nil {
			return []wasmtime.Val{wasmtime.ValI32(-2)}, nil
		}

		data := memory.Memory().UnsafeData(caller)
		if int(keyPtr)+int(keyLen) > len(data) {
			return []wasmtime.Val{wasmtime.ValI32(-2)}, nil
		}

		key := string(data[keyPtr : keyPtr+keyLen])
		val, err := hostFuncs.KVGet(ctx, key)
		if err != nil {
			return []wasmtime.Val{wasmtime.ValI32(-2)}, nil
		}
		if val == nil {
			return []wasmtime.Val{wasmtime.ValI32(-1)}, nil
		}

		if int(outLenPtr)+4 > len(data) {
			return []wasmtime.Val{wasmtime.ValI32(-2)}, nil
		}
		valLen := int32(len(val))
		data[outLenPtr] = byte(valLen)
		data[outLenPtr+1] = byte(valLen >> 8)
		data[outLenPtr+2] = byte(valLen >> 16)
		data[outLenPtr+3] = byte(valLen >> 24)

		if int(outPtr)+len(val) > len(data) {
			return []wasmtime.Val{wasmtime.ValI32(-2)}, nil
		}
		copy(data[outPtr:], val)

		return []wasmtime.Val{wasmtime.ValI32(0)}, nil
	})

	if err := linker.Define(store, "env", "kv_get", kvGetFunc); err != nil {
		return err
	}

	kvSetFuncType := wasmtime.NewFuncType(
		[]*wasmtime.ValType{
			wasmtime.NewValType(wasmtime.KindI32),
			wasmtime.NewValType(wasmtime.KindI32),
			wasmtime.NewValType(wasmtime.KindI32),
			wasmtime.NewValType(wasmtime.KindI32),
		},
		[]*wasmtime.ValType{wasmtime.NewValType(wasmtime.KindI32)},
	)

	kvSetFunc := wasmtime.NewFunc(store, kvSetFuncType, func(caller *wasmtime.Caller, args []wasmtime.Val) ([]wasmtime.Val, *wasmtime.Trap) {
		keyPtr := args[0].I32()
		keyLen := args[1].I32()
		valPtr := args[2].I32()
		valLen := args[3].I32()

		memory := caller.GetExport("memory")
		if memory == nil || memory.Memory() == nil {
			return []wasmtime.Val{wasmtime.ValI32(-1)}, nil
		}

		data := memory.Memory().UnsafeData(caller)
		if int(keyPtr)+int(keyLen) > len(data) || int(valPtr)+int(valLen) > len(data) {
			return []wasmtime.Val{wasmtime.ValI32(-1)}, nil
		}

		key := string(data[keyPtr : keyPtr+keyLen])
		val := make([]byte, valLen)
		copy(val, data[valPtr:valPtr+valLen])

		if err := hostFuncs.KVSet(ctx, key, val); err != nil {
			return []wasmtime.Val{wasmtime.ValI32(-1)}, nil
		}

		return []wasmtime.Val{wasmtime.ValI32(0)}, nil
	})

	if err := linker.Define(store, "env", "kv_set", kvSetFunc); err != nil {
		return err
	}

	kvDeleteFuncType := wasmtime.NewFuncType(
		[]*wasmtime.ValType{
			wasmtime.NewValType(wasmtime.KindI32),
			wasmtime.NewValType(wasmtime.KindI32),
		},
		[]*wasmtime.ValType{wasmtime.NewValType(wasmtime.KindI32)},
	)

	kvDeleteFunc := wasmtime.NewFunc(store, kvDeleteFuncType, func(caller *wasmtime.Caller, args []wasmtime.Val) ([]wasmtime.Val, *wasmtime.Trap) {
		keyPtr := args[0].I32()
		keyLen := args[1].I32()

		memory := caller.GetExport("memory")
		if memory == nil || memory.Memory() == nil {
			return []wasmtime.Val{wasmtime.ValI32(-1)}, nil
		}

		data := memory.Memory().UnsafeData(caller)
		if int(keyPtr)+int(keyLen) > len(data) {
			return []wasmtime.Val{wasmtime.ValI32(-1)}, nil
		}

		key := string(data[keyPtr : keyPtr+keyLen])

		if err := hostFuncs.KVDelete(ctx, key); err != nil {
			return []wasmtime.Val{wasmtime.ValI32(-1)}, nil
		}

		return []wasmtime.Val{wasmtime.ValI32(0)}, nil
	})

	if err := linker.Define(store, "env", "kv_delete", kvDeleteFunc); err != nil {
		return err
	}

	return nil
}

func (r *Runtime) Close() {
	r.cacheMu.Lock()
	r.cache = make(map[string]*CompiledModule)
	r.cacheMu.Unlock()
}
