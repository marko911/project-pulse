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

// RuntimeConfig contains configuration for the WASM runtime.
type RuntimeConfig struct {
	MaxMemoryMB int
	MaxCPUMs    int
	CacheSize   int
}

// ExecutionResult contains the result of a WASM function execution.
type ExecutionResult struct {
	Output      []byte
	DurationMs  int64
	MemoryBytes int64
}

// CompiledModule represents a pre-compiled WASM module.
type CompiledModule struct {
	Module     *wasmtime.Module
	CompiledAt time.Time
}

// Runtime manages WASM execution using wasmtime.
type Runtime struct {
	cfg    RuntimeConfig
	engine *wasmtime.Engine
	logger *slog.Logger

	// Module cache
	cacheMu sync.RWMutex
	cache   map[string]*CompiledModule
}

// NewRuntime creates a new WASM runtime.
func NewRuntime(cfg RuntimeConfig, logger *slog.Logger) (*Runtime, error) {
	// Configure wasmtime engine
	engineCfg := wasmtime.NewConfig()
	engineCfg.SetEpochInterruption(true) // Enable epoch-based interruption for timeouts
	engineCfg.SetConsumeFuel(false)      // Use epochs instead of fuel for simplicity

	engine := wasmtime.NewEngineWithConfig(engineCfg)

	return &Runtime{
		cfg:    cfg,
		engine: engine,
		logger: logger,
		cache:  make(map[string]*CompiledModule),
	}, nil
}

// Compile compiles a WASM module from bytes.
func (r *Runtime) Compile(moduleID string, wasmBytes []byte) (*CompiledModule, error) {
	// Check cache first
	r.cacheMu.RLock()
	if cached, ok := r.cache[moduleID]; ok {
		r.cacheMu.RUnlock()
		return cached, nil
	}
	r.cacheMu.RUnlock()

	// Compile the module
	module, err := wasmtime.NewModule(r.engine, wasmBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to compile module: %w", err)
	}

	compiled := &CompiledModule{
		Module:     module,
		CompiledAt: time.Now(),
	}

	// Cache the compiled module
	r.cacheMu.Lock()
	// Evict oldest if cache is full
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

// Execute runs a compiled WASM module with the given input.
func (r *Runtime) Execute(ctx context.Context, module *CompiledModule, input []byte, hostFuncs *HostFunctions) (*ExecutionResult, error) {
	startTime := time.Now()

	// Create a new store for this execution
	store := wasmtime.NewStore(r.engine)
	defer store.Close()

	// Configure memory limits
	store.Limiter(
		int64(r.cfg.MaxMemoryMB*1024*1024), // Max memory in bytes
		-1,                                  // No table limit
		1,                                   // Max instances
		1,                                   // Max tables
		1,                                   // Max memories
	)

	// Set epoch deadline for CPU time limiting
	store.SetEpochDeadline(1)

	// Set input data on host functions so WASM can retrieve it
	hostFuncs.SetInput(input)

	// Create linker and add WASI + host functions
	linker := wasmtime.NewLinker(r.engine)

	// Add WASI support for Go's wasip1 target
	wasiConfig := wasmtime.NewWasiConfig()
	wasiConfig.InheritEnv()
	store.SetWasi(wasiConfig)
	if err := linker.DefineWasi(); err != nil {
		return nil, fmt.Errorf("failed to define wasi: %w", err)
	}

	if err := r.addHostFunctions(ctx, linker, store, hostFuncs); err != nil {
		return nil, fmt.Errorf("failed to add host functions: %w", err)
	}

	// Instantiate the module
	instance, err := linker.Instantiate(store, module.Module)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate module: %w", err)
	}

	// Get the main function
	mainFunc := instance.GetFunc(store, "_start")
	if mainFunc == nil {
		mainFunc = instance.GetFunc(store, "main")
	}
	if mainFunc == nil {
		return nil, fmt.Errorf("no _start or main function found")
	}

	// Get memory export for later use
	memory := instance.GetExport(store, "memory")

	// Start epoch incrementer for timeout enforcement
	done := make(chan struct{})
	go r.epochIncrementer(ctx, store, done)

	// Execute the function
	_, err = mainFunc.Call(store)
	close(done)

	duration := time.Since(startTime)

	if err != nil {
		// Check if it was a timeout
		if trap, ok := err.(*wasmtime.Trap); ok {
			if trap.Code() != nil && *trap.Code() == wasmtime.Interrupt {
				return nil, fmt.Errorf("execution timeout exceeded %dms", r.cfg.MaxCPUMs)
			}
		}
		// WASI programs exit with proc_exit(0) which triggers a trap
		// Check if this is a successful exit (exit status 0)
		errStr := err.Error()
		if strings.Contains(errStr, "exit status 0") {
			// This is a successful exit, not an error
		} else {
			return nil, fmt.Errorf("execution failed: %w", err)
		}
	}

	// Get memory usage
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

// epochIncrementer increments the epoch periodically to enforce CPU time limits.
func (r *Runtime) epochIncrementer(ctx context.Context, store *wasmtime.Store, done <-chan struct{}) {
	// Calculate tick interval based on max CPU time
	tickInterval := time.Duration(r.cfg.MaxCPUMs) * time.Millisecond / 10
	if tickInterval < time.Millisecond {
		tickInterval = time.Millisecond
	}

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	epochCount := 0
	maxEpochs := 10 // We'll interrupt after 10 epochs

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
			// Force interrupt on context cancellation
			for i := 0; i < maxEpochs; i++ {
				r.engine.IncrementEpoch()
			}
			return
		}
	}
}

// addHostFunctions adds host SDK functions to the linker.
func (r *Runtime) addHostFunctions(ctx context.Context, linker *wasmtime.Linker, store *wasmtime.Store, hostFuncs *HostFunctions) error {
	// Add log function: log(level i32, ptr i32, len i32)
	logFuncType := wasmtime.NewFuncType(
		[]*wasmtime.ValType{
			wasmtime.NewValType(wasmtime.KindI32), // level
			wasmtime.NewValType(wasmtime.KindI32), // ptr
			wasmtime.NewValType(wasmtime.KindI32), // len
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

	// Add output function: output(ptr i32, len i32)
	outputFuncType := wasmtime.NewFuncType(
		[]*wasmtime.ValType{
			wasmtime.NewValType(wasmtime.KindI32), // ptr
			wasmtime.NewValType(wasmtime.KindI32), // len
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

	// Add get_input_len function: get_input_len() -> i32
	// Returns the length of the input data in bytes
	getInputLenFuncType := wasmtime.NewFuncType(
		[]*wasmtime.ValType{},
		[]*wasmtime.ValType{wasmtime.NewValType(wasmtime.KindI32)}, // length
	)

	getInputLenFunc := wasmtime.NewFunc(store, getInputLenFuncType, func(caller *wasmtime.Caller, args []wasmtime.Val) ([]wasmtime.Val, *wasmtime.Trap) {
		return []wasmtime.Val{wasmtime.ValI32(int32(hostFuncs.GetInputLen()))}, nil
	})

	if err := linker.Define(store, "env", "get_input_len", getInputLenFunc); err != nil {
		return err
	}

	// Add get_input function: get_input(ptr i32, len i32) -> i32
	// Copies input data to the specified memory location
	// Returns: number of bytes copied, or -1 on error
	getInputFuncType := wasmtime.NewFuncType(
		[]*wasmtime.ValType{
			wasmtime.NewValType(wasmtime.KindI32), // ptr (destination in WASM memory)
			wasmtime.NewValType(wasmtime.KindI32), // len (max bytes to copy)
		},
		[]*wasmtime.ValType{wasmtime.NewValType(wasmtime.KindI32)}, // bytes copied
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

	// Add kv_get function: kv_get(key_ptr i32, key_len i32, out_ptr i32, out_len_ptr i32) -> i32 (0 = success, -1 = not found, -2 = error)
	kvGetFuncType := wasmtime.NewFuncType(
		[]*wasmtime.ValType{
			wasmtime.NewValType(wasmtime.KindI32), // key_ptr
			wasmtime.NewValType(wasmtime.KindI32), // key_len
			wasmtime.NewValType(wasmtime.KindI32), // out_ptr
			wasmtime.NewValType(wasmtime.KindI32), // out_len_ptr
		},
		[]*wasmtime.ValType{wasmtime.NewValType(wasmtime.KindI32)}, // result
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

		// Write value length to out_len_ptr
		if int(outLenPtr)+4 > len(data) {
			return []wasmtime.Val{wasmtime.ValI32(-2)}, nil
		}
		valLen := int32(len(val))
		data[outLenPtr] = byte(valLen)
		data[outLenPtr+1] = byte(valLen >> 8)
		data[outLenPtr+2] = byte(valLen >> 16)
		data[outLenPtr+3] = byte(valLen >> 24)

		// Write value to out_ptr
		if int(outPtr)+len(val) > len(data) {
			return []wasmtime.Val{wasmtime.ValI32(-2)}, nil
		}
		copy(data[outPtr:], val)

		return []wasmtime.Val{wasmtime.ValI32(0)}, nil
	})

	if err := linker.Define(store, "env", "kv_get", kvGetFunc); err != nil {
		return err
	}

	// Add kv_set function: kv_set(key_ptr i32, key_len i32, val_ptr i32, val_len i32) -> i32 (0 = success, -1 = error)
	kvSetFuncType := wasmtime.NewFuncType(
		[]*wasmtime.ValType{
			wasmtime.NewValType(wasmtime.KindI32), // key_ptr
			wasmtime.NewValType(wasmtime.KindI32), // key_len
			wasmtime.NewValType(wasmtime.KindI32), // val_ptr
			wasmtime.NewValType(wasmtime.KindI32), // val_len
		},
		[]*wasmtime.ValType{wasmtime.NewValType(wasmtime.KindI32)}, // result
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

	// Add kv_delete function: kv_delete(key_ptr i32, key_len i32) -> i32 (0 = success, -1 = error)
	kvDeleteFuncType := wasmtime.NewFuncType(
		[]*wasmtime.ValType{
			wasmtime.NewValType(wasmtime.KindI32), // key_ptr
			wasmtime.NewValType(wasmtime.KindI32), // key_len
		},
		[]*wasmtime.ValType{wasmtime.NewValType(wasmtime.KindI32)}, // result
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

// Close cleans up the runtime resources.
func (r *Runtime) Close() {
	r.cacheMu.Lock()
	r.cache = make(map[string]*CompiledModule)
	r.cacheMu.Unlock()
}
