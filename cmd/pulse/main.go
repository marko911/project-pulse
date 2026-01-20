// Command pulse is the CLI for managing Pulse deployments.
//
// Usage:
//
//	pulse deploy <wasm-file> --name <name> [--version <version>]
//	pulse functions list
//	pulse functions get <id>
//	pulse functions delete <id>
//	pulse triggers list
//	pulse triggers create --function-id <id> --name <name> --event-type <type>
//	pulse triggers get <id>
//	pulse triggers delete <id>
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"
)

var (
	apiEndpoint = envOrDefault("PULSE_API_ENDPOINT", "http://localhost:8090")
	tenantID    = envOrDefault("PULSE_TENANT_ID", "default")
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "deploy":
		deployCmd(os.Args[2:])
	case "functions":
		if len(os.Args) < 3 {
			fmt.Println("Usage: pulse functions <list|get|delete>")
			os.Exit(1)
		}
		functionsCmd(os.Args[2], os.Args[3:])
	case "triggers":
		if len(os.Args) < 3 {
			fmt.Println("Usage: pulse triggers <list|create|get|delete>")
			os.Exit(1)
		}
		triggersCmd(os.Args[2], os.Args[3:])
	case "logs":
		if len(os.Args) < 3 {
			fmt.Println("Usage: pulse logs <function-id> [options]")
			os.Exit(1)
		}
		logsCmd(os.Args[2:])
	case "dev":
		if len(os.Args) < 3 {
			fmt.Println("Usage: pulse dev <wasm-file> [options]")
			os.Exit(1)
		}
		devCmd(os.Args[2:])
	case "help", "--help", "-h":
		printUsage()
	case "version", "--version", "-v":
		fmt.Println("pulse version 0.1.0")
	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`pulse - CLI for Pulse WASM function deployment

Usage:
  pulse deploy <wasm-file> [options]    Deploy a WASM function
  pulse dev <wasm-file> [options]       Watch and auto-deploy on changes
  pulse functions list                  List deployed functions
  pulse functions get <id>              Get function details
  pulse functions delete <id>           Delete a function
  pulse triggers list                   List all triggers
  pulse triggers create [options]       Create a new trigger
  pulse triggers get <id>               Get trigger details
  pulse triggers delete <id>            Delete a trigger
  pulse logs <function-id> [options]    Show function invocation logs
  pulse help                            Show this help
  pulse version                         Show version

Deploy Options:
  --name        Function name (required)
  --version     Function version (default: 1.0.0)
  --description Function description

Dev Options:
  --name        Function name (default: filename without extension)
  --interval    Poll interval in seconds (default: 2)

Trigger Create Options:
  --function-id   Function ID (required)
  --name          Trigger name (required)
  --event-type    Event type to match (required)
  --filter-chain  Chain ID filter (optional)
  --filter-addr   Address filter (optional)
  --priority      Trigger priority (default: 100)
  --max-retries   Max retry attempts (default: 3)
  --timeout-ms    Execution timeout in ms (default: 5000)

Logs Options:
  --limit       Number of logs to show (default: 50, max: 100)
  --errors      Show only failed invocations

Environment Variables:
  PULSE_API_ENDPOINT  API endpoint (default: http://localhost:8090)
  PULSE_TENANT_ID     Tenant ID (default: default)

Examples:
  pulse deploy myfunction.wasm --name my-function
  pulse deploy transform.wasm --name transformer --version 2.0.0
  pulse dev myfunction.wasm --name my-function
  pulse functions list
  pulse functions delete fn_123456789
  pulse triggers list
  pulse triggers create --function-id fn_123 --name my-trigger --event-type Transfer
  pulse triggers delete trg_456789
  pulse logs fn_123456789
  pulse logs fn_123456789 --limit 10 --errors`)
}

func deployCmd(args []string) {
	fs := flag.NewFlagSet("deploy", flag.ExitOnError)
	name := fs.String("name", "", "Function name (required)")
	version := fs.String("version", "1.0.0", "Function version")
	description := fs.String("description", "", "Function description")

	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Println("Error: WASM file path is required")
		fmt.Println("Usage: pulse deploy <wasm-file> --name <name>")
		os.Exit(1)
	}

	wasmPath := fs.Arg(0)

	if *name == "" {
		// Default name from filename
		*name = strings.TrimSuffix(filepath.Base(wasmPath), ".wasm")
	}

	// Validate file exists
	stat, err := os.Stat(wasmPath)
	if err != nil {
		fmt.Printf("Error: cannot access file: %v\n", err)
		os.Exit(1)
	}

	if !strings.HasSuffix(wasmPath, ".wasm") {
		fmt.Println("Error: file must have .wasm extension")
		os.Exit(1)
	}

	fmt.Printf("Deploying %s (%d bytes)...\n", filepath.Base(wasmPath), stat.Size())

	// Open file
	file, err := os.Open(wasmPath)
	if err != nil {
		fmt.Printf("Error: cannot open file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// Create multipart form
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	// Add file
	part, err := writer.CreateFormFile("file", filepath.Base(wasmPath))
	if err != nil {
		fmt.Printf("Error: cannot create form: %v\n", err)
		os.Exit(1)
	}
	if _, err := io.Copy(part, file); err != nil {
		fmt.Printf("Error: cannot read file: %v\n", err)
		os.Exit(1)
	}

	// Add metadata
	writer.WriteField("name", *name)
	writer.WriteField("version", *version)
	if *description != "" {
		writer.WriteField("description", *description)
	}
	writer.Close()

	// Make request
	req, err := http.NewRequest("POST", apiEndpoint+"/api/v1/functions", &buf)
	if err != nil {
		fmt.Printf("Error: cannot create request: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("X-Tenant-ID", tenantID)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusCreated {
		fmt.Printf("Error: deployment failed (%d): %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}

	var result map[string]interface{}
	json.Unmarshal(body, &result)

	fmt.Printf("\n  Function deployed successfully!\n\n")
	fmt.Printf("  ID:      %s\n", result["id"])
	fmt.Printf("  Name:    %s\n", result["name"])
	fmt.Printf("  Version: %s\n", result["version"])
	fmt.Printf("  Size:    %v bytes\n", result["size"])
	fmt.Println()
}

func functionsCmd(subCmd string, args []string) {
	switch subCmd {
	case "list":
		listFunctions()
	case "get":
		if len(args) < 1 {
			fmt.Println("Usage: pulse functions get <id>")
			os.Exit(1)
		}
		getFunction(args[0])
	case "delete":
		if len(args) < 1 {
			fmt.Println("Usage: pulse functions delete <id>")
			os.Exit(1)
		}
		deleteFunction(args[0])
	default:
		fmt.Printf("Unknown subcommand: %s\n", subCmd)
		os.Exit(1)
	}
}

func listFunctions() {
	req, _ := http.NewRequest("GET", apiEndpoint+"/api/v1/functions", nil)
	req.Header.Set("X-Tenant-ID", tenantID)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Error: request failed (%d): %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}

	var result struct {
		Functions []struct {
			ID        string    `json:"id"`
			Name      string    `json:"name"`
			Version   string    `json:"version"`
			Size      int64     `json:"size"`
			CreatedAt time.Time `json:"created_at"`
		} `json:"functions"`
		Count int `json:"count"`
	}

	json.NewDecoder(resp.Body).Decode(&result)

	if result.Count == 0 {
		fmt.Println("No functions deployed yet.")
		fmt.Println("\nDeploy a function with: pulse deploy <wasm-file> --name <name>")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tVERSION\tSIZE\tCREATED")
	fmt.Fprintln(w, "--\t----\t-------\t----\t-------")
	for _, fn := range result.Functions {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\n",
			fn.ID, fn.Name, fn.Version, fn.Size, fn.CreatedAt.Format(time.RFC3339))
	}
	w.Flush()
}

func getFunction(id string) {
	req, _ := http.NewRequest("GET", apiEndpoint+"/api/v1/functions/"+id, nil)
	req.Header.Set("X-Tenant-ID", tenantID)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		fmt.Printf("Function not found: %s\n", id)
		os.Exit(1)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Error: request failed (%d): %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}

	var fn struct {
		ID          string    `json:"id"`
		Name        string    `json:"name"`
		Version     string    `json:"version"`
		Description string    `json:"description"`
		Size        int64     `json:"size"`
		ObjectKey   string    `json:"object_key"`
		CreatedAt   time.Time `json:"created_at"`
		UpdatedAt   time.Time `json:"updated_at"`
	}

	json.NewDecoder(resp.Body).Decode(&fn)

	fmt.Printf("Function: %s\n", fn.Name)
	fmt.Printf("  ID:          %s\n", fn.ID)
	fmt.Printf("  Version:     %s\n", fn.Version)
	if fn.Description != "" {
		fmt.Printf("  Description: %s\n", fn.Description)
	}
	fmt.Printf("  Size:        %d bytes\n", fn.Size)
	fmt.Printf("  Object Key:  %s\n", fn.ObjectKey)
	fmt.Printf("  Created:     %s\n", fn.CreatedAt.Format(time.RFC3339))
	fmt.Printf("  Updated:     %s\n", fn.UpdatedAt.Format(time.RFC3339))
}

func deleteFunction(id string) {
	req, _ := http.NewRequest("DELETE", apiEndpoint+"/api/v1/functions/"+id, nil)
	req.Header.Set("X-Tenant-ID", tenantID)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		fmt.Printf("Function not found: %s\n", id)
		os.Exit(1)
	}

	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Error: delete failed (%d): %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}

	fmt.Printf("Function %s deleted successfully.\n", id)
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// Trigger commands

func triggersCmd(subCmd string, args []string) {
	switch subCmd {
	case "list":
		listTriggers()
	case "create":
		createTrigger(args)
	case "get":
		if len(args) < 1 {
			fmt.Println("Usage: pulse triggers get <id>")
			os.Exit(1)
		}
		getTrigger(args[0])
	case "delete":
		if len(args) < 1 {
			fmt.Println("Usage: pulse triggers delete <id>")
			os.Exit(1)
		}
		deleteTrigger(args[0])
	default:
		fmt.Printf("Unknown subcommand: %s\n", subCmd)
		os.Exit(1)
	}
}

func listTriggers() {
	req, _ := http.NewRequest("GET", apiEndpoint+"/api/v1/triggers", nil)
	req.Header.Set("X-Tenant-ID", tenantID)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Error: request failed (%d): %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}

	var result struct {
		Triggers []struct {
			ID         string    `json:"id"`
			FunctionID string    `json:"function_id"`
			Name       string    `json:"name"`
			EventType  string    `json:"event_type"`
			Status     string    `json:"status"`
			CreatedAt  time.Time `json:"created_at"`
		} `json:"triggers"`
		Count int `json:"count"`
	}

	json.NewDecoder(resp.Body).Decode(&result)

	if result.Count == 0 {
		fmt.Println("No triggers configured yet.")
		fmt.Println("\nCreate a trigger with: pulse triggers create --function-id <id> --name <name> --event-type <type>")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tFUNCTION\tNAME\tEVENT TYPE\tSTATUS\tCREATED")
	fmt.Fprintln(w, "--\t--------\t----\t----------\t------\t-------")
	for _, trg := range result.Triggers {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
			trg.ID, trg.FunctionID, trg.Name, trg.EventType, trg.Status, trg.CreatedAt.Format(time.RFC3339))
	}
	w.Flush()
}

func createTrigger(args []string) {
	fs := flag.NewFlagSet("triggers create", flag.ExitOnError)
	functionID := fs.String("function-id", "", "Function ID (required)")
	name := fs.String("name", "", "Trigger name (required)")
	eventType := fs.String("event-type", "", "Event type to match (required)")
	filterChain := fs.Int("filter-chain", 0, "Chain ID filter (optional)")
	filterAddr := fs.String("filter-addr", "", "Address filter (optional)")
	priority := fs.Int("priority", 100, "Trigger priority")
	maxRetries := fs.Int("max-retries", 3, "Max retry attempts")
	timeoutMs := fs.Int("timeout-ms", 5000, "Execution timeout in ms")

	fs.Parse(args)

	if *functionID == "" {
		fmt.Println("Error: --function-id is required")
		os.Exit(1)
	}
	if *name == "" {
		fmt.Println("Error: --name is required")
		os.Exit(1)
	}
	if *eventType == "" {
		fmt.Println("Error: --event-type is required")
		os.Exit(1)
	}

	// Build request body
	body := map[string]interface{}{
		"name":        *name,
		"event_type":  *eventType,
		"priority":    *priority,
		"max_retries": *maxRetries,
		"timeout_ms":  *timeoutMs,
	}

	if *filterChain > 0 {
		body["filter_chain"] = *filterChain
	}
	if *filterAddr != "" {
		body["filter_address"] = *filterAddr
	}

	jsonBody, _ := json.Marshal(body)

	// Create trigger via function-specific endpoint
	url := fmt.Sprintf("%s/api/v1/functions/%s/triggers", apiEndpoint, *functionID)
	req, err := http.NewRequest("POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		fmt.Printf("Error: cannot create request: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", tenantID)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusCreated {
		fmt.Printf("Error: create failed (%d): %s\n", resp.StatusCode, string(respBody))
		os.Exit(1)
	}

	var result map[string]interface{}
	json.Unmarshal(respBody, &result)

	fmt.Printf("\n  Trigger created successfully!\n\n")
	fmt.Printf("  ID:          %s\n", result["id"])
	fmt.Printf("  Name:        %s\n", result["name"])
	fmt.Printf("  Function ID: %s\n", result["function_id"])
	fmt.Printf("  Event Type:  %s\n", result["event_type"])
	fmt.Printf("  Status:      %s\n", result["status"])
	fmt.Println()
}

func getTrigger(id string) {
	req, _ := http.NewRequest("GET", apiEndpoint+"/api/v1/triggers/"+id, nil)
	req.Header.Set("X-Tenant-ID", tenantID)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		fmt.Printf("Trigger not found: %s\n", id)
		os.Exit(1)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Error: request failed (%d): %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}

	var trg struct {
		ID            string    `json:"id"`
		FunctionID    string    `json:"function_id"`
		TenantID      string    `json:"tenant_id"`
		Name          string    `json:"name"`
		EventType     string    `json:"event_type"`
		FilterChain   *int      `json:"filter_chain"`
		FilterAddress *string   `json:"filter_address"`
		FilterTopic   *string   `json:"filter_topic"`
		Priority      int       `json:"priority"`
		MaxRetries    int       `json:"max_retries"`
		TimeoutMs     int       `json:"timeout_ms"`
		Status        string    `json:"status"`
		CreatedAt     time.Time `json:"created_at"`
		UpdatedAt     time.Time `json:"updated_at"`
	}

	json.NewDecoder(resp.Body).Decode(&trg)

	fmt.Printf("Trigger: %s\n", trg.Name)
	fmt.Printf("  ID:          %s\n", trg.ID)
	fmt.Printf("  Function ID: %s\n", trg.FunctionID)
	fmt.Printf("  Tenant ID:   %s\n", trg.TenantID)
	fmt.Printf("  Event Type:  %s\n", trg.EventType)
	if trg.FilterChain != nil {
		fmt.Printf("  Filter Chain:   %d\n", *trg.FilterChain)
	}
	if trg.FilterAddress != nil {
		fmt.Printf("  Filter Address: %s\n", *trg.FilterAddress)
	}
	if trg.FilterTopic != nil {
		fmt.Printf("  Filter Topic:   %s\n", *trg.FilterTopic)
	}
	fmt.Printf("  Priority:    %d\n", trg.Priority)
	fmt.Printf("  Max Retries: %d\n", trg.MaxRetries)
	fmt.Printf("  Timeout:     %d ms\n", trg.TimeoutMs)
	fmt.Printf("  Status:      %s\n", trg.Status)
	fmt.Printf("  Created:     %s\n", trg.CreatedAt.Format(time.RFC3339))
	fmt.Printf("  Updated:     %s\n", trg.UpdatedAt.Format(time.RFC3339))
}

func deleteTrigger(id string) {
	req, _ := http.NewRequest("DELETE", apiEndpoint+"/api/v1/triggers/"+id, nil)
	req.Header.Set("X-Tenant-ID", tenantID)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		fmt.Printf("Trigger not found: %s\n", id)
		os.Exit(1)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Error: delete failed (%d): %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}

	fmt.Printf("Trigger %s deleted successfully.\n", id)
}

// Logs command

func logsCmd(args []string) {
	fs := flag.NewFlagSet("logs", flag.ExitOnError)
	limit := fs.Int("limit", 50, "Number of logs to show (max: 100)")
	errorsOnly := fs.Bool("errors", false, "Show only failed invocations")

	// Parse flags after the function ID
	functionID := args[0]
	fs.Parse(args[1:])

	if *limit > 100 {
		*limit = 100
	}

	getLogs(functionID, *limit, *errorsOnly)
}

func getLogs(functionID string, limit int, errorsOnly bool) {
	url := fmt.Sprintf("%s/api/v1/functions/%s/logs?limit=%d", apiEndpoint, functionID, limit)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("X-Tenant-ID", tenantID)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		fmt.Printf("Function not found: %s\n", functionID)
		os.Exit(1)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Error: request failed (%d): %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}

	var result struct {
		FunctionID  string `json:"function_id"`
		Invocations []struct {
			ID           string    `json:"id"`
			RequestID    string    `json:"request_id"`
			Success      bool      `json:"success"`
			DurationMs   int       `json:"duration_ms"`
			ErrorMessage *string   `json:"error_message"`
			MemoryBytes  *int64    `json:"memory_bytes"`
			StartedAt    time.Time `json:"started_at"`
			CompletedAt  time.Time `json:"completed_at"`
		} `json:"invocations"`
		Count int `json:"count"`
	}

	json.NewDecoder(resp.Body).Decode(&result)

	if result.Count == 0 {
		fmt.Printf("No invocations found for function %s.\n", functionID)
		return
	}

	// Filter errors if requested
	invocations := result.Invocations
	if errorsOnly {
		filtered := make([]struct {
			ID           string    `json:"id"`
			RequestID    string    `json:"request_id"`
			Success      bool      `json:"success"`
			DurationMs   int       `json:"duration_ms"`
			ErrorMessage *string   `json:"error_message"`
			MemoryBytes  *int64    `json:"memory_bytes"`
			StartedAt    time.Time `json:"started_at"`
			CompletedAt  time.Time `json:"completed_at"`
		}, 0)
		for _, inv := range invocations {
			if !inv.Success {
				filtered = append(filtered, inv)
			}
		}
		invocations = filtered

		if len(invocations) == 0 {
			fmt.Printf("No failed invocations found for function %s.\n", functionID)
			return
		}
	}

	fmt.Printf("Invocation logs for function %s:\n\n", functionID)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "REQUEST ID\tSTATUS\tDURATION\tMEMORY\tCOMPLETED\tERROR")
	fmt.Fprintln(w, "----------\t------\t--------\t------\t---------\t-----")

	for _, inv := range invocations {
		status := "✓"
		if !inv.Success {
			status = "✗"
		}

		memory := "-"
		if inv.MemoryBytes != nil {
			memory = formatBytes(*inv.MemoryBytes)
		}

		errorMsg := ""
		if inv.ErrorMessage != nil {
			errorMsg = *inv.ErrorMessage
			if len(errorMsg) > 40 {
				errorMsg = errorMsg[:37] + "..."
			}
		}

		fmt.Fprintf(w, "%s\t%s\t%dms\t%s\t%s\t%s\n",
			inv.RequestID,
			status,
			inv.DurationMs,
			memory,
			inv.CompletedAt.Format("15:04:05"),
			errorMsg,
		)
	}
	w.Flush()

	fmt.Printf("\nShowing %d of %d invocations.\n", len(invocations), result.Count)
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%dB", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// Dev command - watch and auto-deploy

func devCmd(args []string) {
	fs := flag.NewFlagSet("dev", flag.ExitOnError)
	name := fs.String("name", "", "Function name (default: filename without extension)")
	interval := fs.Int("interval", 2, "Poll interval in seconds")

	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Println("Error: WASM file path is required")
		fmt.Println("Usage: pulse dev <wasm-file> [--name <name>] [--interval <seconds>]")
		os.Exit(1)
	}

	wasmPath := fs.Arg(0)

	// Validate file exists
	stat, err := os.Stat(wasmPath)
	if err != nil {
		fmt.Printf("Error: cannot access file: %v\n", err)
		os.Exit(1)
	}

	if !strings.HasSuffix(wasmPath, ".wasm") {
		fmt.Println("Error: file must have .wasm extension")
		os.Exit(1)
	}

	if *name == "" {
		*name = strings.TrimSuffix(filepath.Base(wasmPath), ".wasm")
	}

	fmt.Printf("🔄 Dev mode: watching %s\n", wasmPath)
	fmt.Printf("   Function name: %s\n", *name)
	fmt.Printf("   Poll interval: %ds\n", *interval)
	fmt.Println("   Press Ctrl+C to stop")

	// Get absolute path for consistency
	absPath, err := filepath.Abs(wasmPath)
	if err != nil {
		absPath = wasmPath
	}

	// Track last modification time and hash
	lastModTime := stat.ModTime()
	lastHash := hashFile(absPath)
	deployCount := 0

	// Initial deploy
	fmt.Println("📦 Initial deployment...")
	if err := deployFile(absPath, *name); err != nil {
		fmt.Printf("❌ Deploy failed: %v\n", err)
	} else {
		deployCount++
		fmt.Printf("✅ Deployed v%d\n\n", deployCount)
	}

	// Watch loop
	ticker := time.NewTicker(time.Duration(*interval) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		stat, err := os.Stat(absPath)
		if err != nil {
			fmt.Printf("⚠️  Cannot access file: %v\n", err)
			continue
		}

		// Check if file was modified
		if stat.ModTime().After(lastModTime) {
			// Verify content actually changed (not just touch)
			newHash := hashFile(absPath)
			if newHash != lastHash {
				lastModTime = stat.ModTime()
				lastHash = newHash

				fmt.Printf("\n🔄 Change detected at %s\n", time.Now().Format("15:04:05"))
				fmt.Printf("📦 Deploying %s (%d bytes)...\n", filepath.Base(absPath), stat.Size())

				if err := deployFile(absPath, *name); err != nil {
					fmt.Printf("❌ Deploy failed: %v\n", err)
				} else {
					deployCount++
					fmt.Printf("✅ Deployed v%d\n", deployCount)
				}
			} else {
				lastModTime = stat.ModTime()
			}
		}
	}
}

func hashFile(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:8]) // First 8 bytes is enough for change detection
}

func deployFile(wasmPath, name string) error {
	file, err := os.Open(wasmPath)
	if err != nil {
		return fmt.Errorf("cannot open file: %w", err)
	}
	defer file.Close()

	// Create multipart form
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	part, err := writer.CreateFormFile("file", filepath.Base(wasmPath))
	if err != nil {
		return fmt.Errorf("cannot create form: %w", err)
	}
	if _, err := io.Copy(part, file); err != nil {
		return fmt.Errorf("cannot read file: %w", err)
	}

	writer.WriteField("name", name)
	writer.WriteField("version", "dev")
	writer.Close()

	// Make request
	req, err := http.NewRequest("POST", apiEndpoint+"/api/v1/functions", &buf)
	if err != nil {
		return fmt.Errorf("cannot create request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("X-Tenant-ID", tenantID)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("deployment failed (%d): %s", resp.StatusCode, string(body))
	}

	return nil
}
