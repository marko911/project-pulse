package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type FunctionRepository struct {
	db *DB
}

func NewFunctionRepository(db *DB) *FunctionRepository {
	return &FunctionRepository{db: db}
}

func (r *FunctionRepository) CreateFunction(ctx context.Context, f *Function) error {
	sql := `
		INSERT INTO functions (
			tenant_id, name, description, runtime_version,
			max_memory_mb, max_cpu_ms, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, current_version, created_at, updated_at
	`

	return r.db.pool.QueryRow(ctx, sql,
		f.TenantID, f.Name, f.Description, f.RuntimeVersion,
		f.MaxMemoryMB, f.MaxCPUMs, f.Status,
	).Scan(&f.ID, &f.CurrentVersion, &f.CreatedAt, &f.UpdatedAt)
}

func (r *FunctionRepository) GetFunction(ctx context.Context, id string) (*Function, error) {
	sql := `
		SELECT id, tenant_id, name, description, runtime_version,
		       max_memory_mb, max_cpu_ms, current_version, module_hash,
		       module_size, status, created_at, updated_at
		FROM functions
		WHERE id = $1 AND status != 'deleted'
	`

	var f Function
	err := r.db.pool.QueryRow(ctx, sql, id).Scan(
		&f.ID, &f.TenantID, &f.Name, &f.Description, &f.RuntimeVersion,
		&f.MaxMemoryMB, &f.MaxCPUMs, &f.CurrentVersion, &f.ModuleHash,
		&f.ModuleSize, &f.Status, &f.CreatedAt, &f.UpdatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query function: %w", err)
	}

	return &f, nil
}

func (r *FunctionRepository) GetFunctionByName(ctx context.Context, tenantID, name string) (*Function, error) {
	sql := `
		SELECT id, tenant_id, name, description, runtime_version,
		       max_memory_mb, max_cpu_ms, current_version, module_hash,
		       module_size, status, created_at, updated_at
		FROM functions
		WHERE tenant_id = $1 AND name = $2 AND status != 'deleted'
	`

	var f Function
	err := r.db.pool.QueryRow(ctx, sql, tenantID, name).Scan(
		&f.ID, &f.TenantID, &f.Name, &f.Description, &f.RuntimeVersion,
		&f.MaxMemoryMB, &f.MaxCPUMs, &f.CurrentVersion, &f.ModuleHash,
		&f.ModuleSize, &f.Status, &f.CreatedAt, &f.UpdatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query function by name: %w", err)
	}

	return &f, nil
}

func (r *FunctionRepository) ListFunctions(ctx context.Context, tenantID string, limit, offset int) ([]Function, error) {
	sql := `
		SELECT id, tenant_id, name, description, runtime_version,
		       max_memory_mb, max_cpu_ms, current_version, module_hash,
		       module_size, status, created_at, updated_at
		FROM functions
		WHERE tenant_id = $1 AND status != 'deleted'
		ORDER BY updated_at DESC
		LIMIT $2 OFFSET $3
	`

	rows, err := r.db.pool.Query(ctx, sql, tenantID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("query functions: %w", err)
	}
	defer rows.Close()

	var functions []Function
	for rows.Next() {
		var f Function
		err := rows.Scan(
			&f.ID, &f.TenantID, &f.Name, &f.Description, &f.RuntimeVersion,
			&f.MaxMemoryMB, &f.MaxCPUMs, &f.CurrentVersion, &f.ModuleHash,
			&f.ModuleSize, &f.Status, &f.CreatedAt, &f.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan function: %w", err)
		}
		functions = append(functions, f)
	}

	return functions, rows.Err()
}

func (r *FunctionRepository) UpdateFunction(ctx context.Context, f *Function) error {
	sql := `
		UPDATE functions
		SET name = $2, description = $3, runtime_version = $4,
		    max_memory_mb = $5, max_cpu_ms = $6, status = $7, updated_at = NOW()
		WHERE id = $1 AND status != 'deleted'
		RETURNING updated_at
	`

	err := r.db.pool.QueryRow(ctx, sql,
		f.ID, f.Name, f.Description, f.RuntimeVersion,
		f.MaxMemoryMB, f.MaxCPUMs, f.Status,
	).Scan(&f.UpdatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return fmt.Errorf("function not found")
		}
		return fmt.Errorf("update function: %w", err)
	}

	return nil
}

func (r *FunctionRepository) DeleteFunction(ctx context.Context, id string) error {
	sql := `
		UPDATE functions
		SET status = 'deleted', updated_at = NOW()
		WHERE id = $1 AND status != 'deleted'
	`

	result, err := r.db.pool.Exec(ctx, sql, id)
	if err != nil {
		return fmt.Errorf("delete function: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("function not found")
	}

	return nil
}

func (r *FunctionRepository) CreateTrigger(ctx context.Context, t *Trigger) error {
	sql := `
		INSERT INTO triggers (
			function_id, tenant_id, name, event_type, filter_chain,
			filter_address, filter_topic, filter_json, priority,
			max_retries, timeout_ms, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		RETURNING id, created_at, updated_at
	`

	return r.db.pool.QueryRow(ctx, sql,
		t.FunctionID, t.TenantID, t.Name, t.EventType, t.FilterChain,
		t.FilterAddress, t.FilterTopic, t.FilterJSON, t.Priority,
		t.MaxRetries, t.TimeoutMs, t.Status,
	).Scan(&t.ID, &t.CreatedAt, &t.UpdatedAt)
}

func (r *FunctionRepository) GetTrigger(ctx context.Context, id string) (*Trigger, error) {
	sql := `
		SELECT id, function_id, tenant_id, name, event_type, filter_chain,
		       filter_address, filter_topic, filter_json, priority,
		       max_retries, timeout_ms, status, created_at, updated_at
		FROM triggers
		WHERE id = $1 AND status != 'deleted'
	`

	var t Trigger
	err := r.db.pool.QueryRow(ctx, sql, id).Scan(
		&t.ID, &t.FunctionID, &t.TenantID, &t.Name, &t.EventType, &t.FilterChain,
		&t.FilterAddress, &t.FilterTopic, &t.FilterJSON, &t.Priority,
		&t.MaxRetries, &t.TimeoutMs, &t.Status, &t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query trigger: %w", err)
	}

	return &t, nil
}

func (r *FunctionRepository) ListTriggersByFunction(ctx context.Context, functionID string) ([]Trigger, error) {
	sql := `
		SELECT id, function_id, tenant_id, name, event_type, filter_chain,
		       filter_address, filter_topic, filter_json, priority,
		       max_retries, timeout_ms, status, created_at, updated_at
		FROM triggers
		WHERE function_id = $1 AND status != 'deleted'
		ORDER BY priority ASC, created_at ASC
	`

	rows, err := r.db.pool.Query(ctx, sql, functionID)
	if err != nil {
		return nil, fmt.Errorf("query triggers: %w", err)
	}
	defer rows.Close()

	var triggers []Trigger
	for rows.Next() {
		var t Trigger
		err := rows.Scan(
			&t.ID, &t.FunctionID, &t.TenantID, &t.Name, &t.EventType, &t.FilterChain,
			&t.FilterAddress, &t.FilterTopic, &t.FilterJSON, &t.Priority,
			&t.MaxRetries, &t.TimeoutMs, &t.Status, &t.CreatedAt, &t.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan trigger: %w", err)
		}
		triggers = append(triggers, t)
	}

	return triggers, rows.Err()
}

func (r *FunctionRepository) FindMatchingTriggers(ctx context.Context, eventType string, chain *int16, address *string) ([]Trigger, error) {
	sql := `
		SELECT id, function_id, tenant_id, name, event_type, filter_chain,
		       filter_address, filter_topic, filter_json, priority,
		       max_retries, timeout_ms, status, created_at, updated_at
		FROM triggers
		WHERE status = 'active'
		  AND event_type = $1
		  AND (filter_chain IS NULL OR filter_chain = $2)
		  AND (filter_address IS NULL OR filter_address = $3)
		ORDER BY priority ASC
	`

	rows, err := r.db.pool.Query(ctx, sql, eventType, chain, address)
	if err != nil {
		return nil, fmt.Errorf("query matching triggers: %w", err)
	}
	defer rows.Close()

	var triggers []Trigger
	for rows.Next() {
		var t Trigger
		err := rows.Scan(
			&t.ID, &t.FunctionID, &t.TenantID, &t.Name, &t.EventType, &t.FilterChain,
			&t.FilterAddress, &t.FilterTopic, &t.FilterJSON, &t.Priority,
			&t.MaxRetries, &t.TimeoutMs, &t.Status, &t.CreatedAt, &t.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan trigger: %w", err)
		}
		triggers = append(triggers, t)
	}

	return triggers, rows.Err()
}

func (r *FunctionRepository) DeleteTrigger(ctx context.Context, id string) error {
	sql := `
		UPDATE triggers
		SET status = 'deleted', updated_at = NOW()
		WHERE id = $1 AND status != 'deleted'
	`

	result, err := r.db.pool.Exec(ctx, sql, id)
	if err != nil {
		return fmt.Errorf("delete trigger: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("trigger not found")
	}

	return nil
}

func (r *FunctionRepository) ListTriggersByTenant(ctx context.Context, tenantID string, limit, offset int) ([]Trigger, error) {
	sql := `
		SELECT id, function_id, tenant_id, name, event_type, filter_chain,
		       filter_address, filter_topic, filter_json, priority,
		       max_retries, timeout_ms, status, created_at, updated_at
		FROM triggers
		WHERE tenant_id = $1 AND status != 'deleted'
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`

	rows, err := r.db.pool.Query(ctx, sql, tenantID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("query triggers by tenant: %w", err)
	}
	defer rows.Close()

	var triggers []Trigger
	for rows.Next() {
		var t Trigger
		err := rows.Scan(
			&t.ID, &t.FunctionID, &t.TenantID, &t.Name, &t.EventType, &t.FilterChain,
			&t.FilterAddress, &t.FilterTopic, &t.FilterJSON, &t.Priority,
			&t.MaxRetries, &t.TimeoutMs, &t.Status, &t.CreatedAt, &t.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan trigger: %w", err)
		}
		triggers = append(triggers, t)
	}

	return triggers, rows.Err()
}

func (r *FunctionRepository) UpdateTrigger(ctx context.Context, t *Trigger) error {
	sql := `
		UPDATE triggers
		SET name = $2, event_type = $3, filter_chain = $4, filter_address = $5,
		    filter_topic = $6, filter_json = $7, priority = $8, max_retries = $9,
		    timeout_ms = $10, status = $11, updated_at = NOW()
		WHERE id = $1 AND status != 'deleted'
		RETURNING updated_at
	`

	err := r.db.pool.QueryRow(ctx, sql,
		t.ID, t.Name, t.EventType, t.FilterChain, t.FilterAddress,
		t.FilterTopic, t.FilterJSON, t.Priority, t.MaxRetries,
		t.TimeoutMs, t.Status,
	).Scan(&t.UpdatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return fmt.Errorf("trigger not found")
		}
		return fmt.Errorf("update trigger: %w", err)
	}

	return nil
}

func (r *FunctionRepository) CreateDeployment(ctx context.Context, d *Deployment) error {
	return r.db.WithTx(ctx, func(tx pgx.Tx) error {
		var nextVersion int
		err := tx.QueryRow(ctx, `
			UPDATE functions
			SET current_version = current_version + 1,
			    module_hash = $2,
			    module_size = $3,
			    updated_at = NOW()
			WHERE id = $1 AND status != 'deleted'
			RETURNING current_version
		`, d.FunctionID, d.ModuleHash, d.ModuleSize).Scan(&nextVersion)
		if err != nil {
			return fmt.Errorf("update function version: %w", err)
		}

		d.Version = nextVersion

		_, err = tx.Exec(ctx, `
			UPDATE deployments
			SET status = 'superseded'
			WHERE function_id = $1 AND status = 'active'
		`, d.FunctionID)
		if err != nil {
			return fmt.Errorf("supersede old deployments: %w", err)
		}

		sql := `
			INSERT INTO deployments (
				function_id, tenant_id, version, module_hash, module_size,
				module_path, source_hash, build_log, deployed_by,
				deployment_note, status, activated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			RETURNING id, created_at
		`

		activatedAt := time.Now().UTC()
		d.ActivatedAt = &activatedAt

		return tx.QueryRow(ctx, sql,
			d.FunctionID, d.TenantID, d.Version, d.ModuleHash, d.ModuleSize,
			d.ModulePath, d.SourceHash, d.BuildLog, d.DeployedBy,
			d.DeploymentNote, d.Status, d.ActivatedAt,
		).Scan(&d.ID, &d.CreatedAt)
	})
}

func (r *FunctionRepository) GetDeployment(ctx context.Context, id string) (*Deployment, error) {
	sql := `
		SELECT id, function_id, tenant_id, version, module_hash, module_size,
		       module_path, source_hash, build_log, deployed_by,
		       deployment_note, status, created_at, activated_at
		FROM deployments
		WHERE id = $1
	`

	var d Deployment
	err := r.db.pool.QueryRow(ctx, sql, id).Scan(
		&d.ID, &d.FunctionID, &d.TenantID, &d.Version, &d.ModuleHash, &d.ModuleSize,
		&d.ModulePath, &d.SourceHash, &d.BuildLog, &d.DeployedBy,
		&d.DeploymentNote, &d.Status, &d.CreatedAt, &d.ActivatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query deployment: %w", err)
	}

	return &d, nil
}

func (r *FunctionRepository) GetActiveDeployment(ctx context.Context, functionID string) (*Deployment, error) {
	sql := `
		SELECT id, function_id, tenant_id, version, module_hash, module_size,
		       module_path, source_hash, build_log, deployed_by,
		       deployment_note, status, created_at, activated_at
		FROM deployments
		WHERE function_id = $1 AND status = 'active'
	`

	var d Deployment
	err := r.db.pool.QueryRow(ctx, sql, functionID).Scan(
		&d.ID, &d.FunctionID, &d.TenantID, &d.Version, &d.ModuleHash, &d.ModuleSize,
		&d.ModulePath, &d.SourceHash, &d.BuildLog, &d.DeployedBy,
		&d.DeploymentNote, &d.Status, &d.CreatedAt, &d.ActivatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query active deployment: %w", err)
	}

	return &d, nil
}

func (r *FunctionRepository) ListDeployments(ctx context.Context, functionID string, limit int) ([]Deployment, error) {
	sql := `
		SELECT id, function_id, tenant_id, version, module_hash, module_size,
		       module_path, source_hash, build_log, deployed_by,
		       deployment_note, status, created_at, activated_at
		FROM deployments
		WHERE function_id = $1
		ORDER BY version DESC
		LIMIT $2
	`

	rows, err := r.db.pool.Query(ctx, sql, functionID, limit)
	if err != nil {
		return nil, fmt.Errorf("query deployments: %w", err)
	}
	defer rows.Close()

	var deployments []Deployment
	for rows.Next() {
		var d Deployment
		err := rows.Scan(
			&d.ID, &d.FunctionID, &d.TenantID, &d.Version, &d.ModuleHash, &d.ModuleSize,
			&d.ModulePath, &d.SourceHash, &d.BuildLog, &d.DeployedBy,
			&d.DeploymentNote, &d.Status, &d.CreatedAt, &d.ActivatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan deployment: %w", err)
		}
		deployments = append(deployments, d)
	}

	return deployments, rows.Err()
}

func (r *FunctionRepository) RollbackDeployment(ctx context.Context, functionID string, targetVersion int) (*Deployment, error) {
	var d Deployment

	err := r.db.WithTx(ctx, func(tx pgx.Tx) error {
		sql := `
			SELECT id, function_id, tenant_id, version, module_hash, module_size,
			       module_path, source_hash, build_log, deployed_by,
			       deployment_note, status, created_at, activated_at
			FROM deployments
			WHERE function_id = $1 AND version = $2
		`
		err := tx.QueryRow(ctx, sql, functionID, targetVersion).Scan(
			&d.ID, &d.FunctionID, &d.TenantID, &d.Version, &d.ModuleHash, &d.ModuleSize,
			&d.ModulePath, &d.SourceHash, &d.BuildLog, &d.DeployedBy,
			&d.DeploymentNote, &d.Status, &d.CreatedAt, &d.ActivatedAt,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return fmt.Errorf("deployment version %d not found", targetVersion)
			}
			return fmt.Errorf("query target deployment: %w", err)
		}

		_, err = tx.Exec(ctx, `
			UPDATE deployments
			SET status = 'rollback'
			WHERE function_id = $1 AND status = 'active'
		`, functionID)
		if err != nil {
			return fmt.Errorf("mark current as rollback: %w", err)
		}

		activatedAt := time.Now().UTC()
		_, err = tx.Exec(ctx, `
			UPDATE deployments
			SET status = 'active', activated_at = $2
			WHERE id = $1
		`, d.ID, activatedAt)
		if err != nil {
			return fmt.Errorf("activate target deployment: %w", err)
		}

		d.Status = DeploymentStatusActive
		d.ActivatedAt = &activatedAt

		_, err = tx.Exec(ctx, `
			UPDATE functions
			SET module_hash = $2, module_size = $3, updated_at = NOW()
			WHERE id = $1
		`, functionID, d.ModuleHash, d.ModuleSize)
		if err != nil {
			return fmt.Errorf("update function after rollback: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &d, nil
}

func (r *FunctionRepository) RecordInvocation(ctx context.Context, inv *Invocation) error {
	sql := `
		INSERT INTO invocations (
			function_id, trigger_id, tenant_id, deployment_id, request_id,
			input_hash, input_size, success, output_hash, output_size,
			error_message, duration_ms, memory_bytes, cpu_time_ms,
			started_at, completed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		RETURNING id
	`

	return r.db.pool.QueryRow(ctx, sql,
		inv.FunctionID, inv.TriggerID, inv.TenantID, inv.DeploymentID, inv.RequestID,
		inv.InputHash, inv.InputSize, inv.Success, inv.OutputHash, inv.OutputSize,
		inv.ErrorMessage, inv.DurationMs, inv.MemoryBytes, inv.CPUTimeMs,
		inv.StartedAt, inv.CompletedAt,
	).Scan(&inv.ID)
}

func (r *FunctionRepository) GetInvocationStats(ctx context.Context, functionID string, since time.Time) (*InvocationStats, error) {
	sql := `
		SELECT
			COUNT(*) as total_invocations,
			COUNT(*) FILTER (WHERE success = true) as successful_invocations,
			COUNT(*) FILTER (WHERE success = false) as failed_invocations,
			COALESCE(AVG(duration_ms), 0) as avg_duration_ms,
			COALESCE(MAX(duration_ms), 0) as max_duration_ms,
			COALESCE(AVG(memory_bytes), 0) as avg_memory_bytes
		FROM invocations
		WHERE function_id = $1 AND completed_at >= $2
	`

	var stats InvocationStats
	err := r.db.pool.QueryRow(ctx, sql, functionID, since).Scan(
		&stats.TotalInvocations,
		&stats.SuccessfulInvocations,
		&stats.FailedInvocations,
		&stats.AvgDurationMs,
		&stats.MaxDurationMs,
		&stats.AvgMemoryBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("query invocation stats: %w", err)
	}

	return &stats, nil
}

type InvocationStats struct {
	TotalInvocations      int64   `json:"total_invocations"`
	SuccessfulInvocations int64   `json:"successful_invocations"`
	FailedInvocations     int64   `json:"failed_invocations"`
	AvgDurationMs         float64 `json:"avg_duration_ms"`
	MaxDurationMs         int     `json:"max_duration_ms"`
	AvgMemoryBytes        float64 `json:"avg_memory_bytes"`
}

func (r *FunctionRepository) ListInvocations(ctx context.Context, functionID string, limit, offset int) ([]Invocation, error) {
	sql := `
		SELECT id, function_id, trigger_id, tenant_id, deployment_id, request_id,
		       input_hash, input_size, success, output_hash, output_size,
		       error_message, duration_ms, memory_bytes, cpu_time_ms,
		       started_at, completed_at
		FROM invocations
		WHERE function_id = $1
		ORDER BY completed_at DESC
		LIMIT $2 OFFSET $3
	`

	rows, err := r.db.pool.Query(ctx, sql, functionID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("query invocations: %w", err)
	}
	defer rows.Close()

	var invocations []Invocation
	for rows.Next() {
		var inv Invocation
		err := rows.Scan(
			&inv.ID, &inv.FunctionID, &inv.TriggerID, &inv.TenantID, &inv.DeploymentID,
			&inv.RequestID, &inv.InputHash, &inv.InputSize, &inv.Success,
			&inv.OutputHash, &inv.OutputSize, &inv.ErrorMessage, &inv.DurationMs,
			&inv.MemoryBytes, &inv.CPUTimeMs, &inv.StartedAt, &inv.CompletedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan invocation: %w", err)
		}
		invocations = append(invocations, inv)
	}

	return invocations, rows.Err()
}
