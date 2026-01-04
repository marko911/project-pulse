package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"unsafe"
)

// Import host functions from the "env" module
//
//go:wasmimport env log
func hostLog(level int32, ptr unsafe.Pointer, len int32)

//go:wasmimport env output
func hostOutput(ptr unsafe.Pointer, len int32)

//go:wasmimport env get_input_len
func hostGetInputLen() int32

//go:wasmimport env get_input
func hostGetInput(ptr unsafe.Pointer, len int32) int32

// Helper to log a string
func log(level int, msg string) {
	ptr := unsafe.Pointer(unsafe.StringData(msg))
	hostLog(int32(level), ptr, int32(len(msg)))
}

// Helper to set output
func setOutput(msg string) {
	ptr := unsafe.Pointer(unsafe.StringData(msg))
	hostOutput(ptr, int32(len(msg)))
}

// Helper to read the input event data
func getInput() []byte {
	length := hostGetInputLen()
	if length <= 0 {
		return nil
	}
	buf := make([]byte, length)
	ptr := unsafe.Pointer(&buf[0])
	read := hostGetInput(ptr, length)
	if read <= 0 {
		return nil
	}
	return buf[:read]
}

// CanonicalEvent represents the event structure from the platform
type CanonicalEvent struct {
	EventID    string `json:"event_id"`
	Chain      int    `json:"chain"`
	BlockNum   uint64 `json:"block_number"`
	TxHash     string `json:"tx_hash"`
	EventType  string `json:"event_type"`
	Timestamp  string `json:"timestamp"`
	Payload    string `json:"payload"` // Base64 encoded
}

// SolanaLogsPayload is the decoded payload structure from Solana logsSubscribe
type SolanaLogsPayload struct {
	Result struct {
		Context struct {
			Slot uint64 `json:"slot"`
		} `json:"context"`
		Value struct {
			Signature string   `json:"signature"`
			Err       any      `json:"err"`
			Logs      []string `json:"logs"`
		} `json:"value"`
	} `json:"result"`
}

// Known program IDs for display
var programNames = map[string]string{
	"11111111111111111111111111111111":             "System Program",
	"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA": "Token Program",
	"ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL": "Associated Token",
	"ComputeBudget111111111111111111111111111111":  "Compute Budget",
	"dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH": "Drift Protocol",
	"JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4":  "Jupiter v6",
	"whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc":  "Orca Whirlpool",
	"675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "Raydium AMM",
	"cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG": "CPAMM",
	"metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s":  "Metaplex",
}

func main() {
	// Read the incoming event
	eventData := getInput()
	if eventData == nil {
		log(2, "No event data received")
		setOutput(`{"status": "error", "message": "No event data"}`)
		return
	}

	// Parse the canonical event
	var event CanonicalEvent
	if err := json.Unmarshal(eventData, &event); err != nil {
		log(2, "Failed to parse event: "+err.Error())
		setOutput(`{"status": "error", "message": "Failed to parse event"}`)
		return
	}

	log(1, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log(1, fmt.Sprintf("📦 Solana Transaction @ Slot %d", event.BlockNum))
	log(1, fmt.Sprintf("🔗 Signature: %s", truncate(event.TxHash, 20)))

	// Decode the base64 payload
	if event.Payload == "" {
		log(2, "No payload in event")
		setOutput(`{"status": "ok", "message": "No payload"}`)
		return
	}

	payloadBytes, err := base64.StdEncoding.DecodeString(event.Payload)
	if err != nil {
		log(2, "Failed to decode payload: "+err.Error())
		setOutput(`{"status": "error", "message": "Failed to decode payload"}`)
		return
	}

	// Parse the Solana logs payload
	var solanaPayload SolanaLogsPayload
	if err := json.Unmarshal(payloadBytes, &solanaPayload); err != nil {
		log(2, "Failed to parse Solana payload: "+err.Error())
		setOutput(`{"status": "error", "message": "Failed to parse Solana payload"}`)
		return
	}

	// Check for errors
	if solanaPayload.Result.Value.Err != nil {
		log(2, fmt.Sprintf("❌ Transaction FAILED: %v", solanaPayload.Result.Value.Err))
	}

	// Analyze the logs
	programs := make(map[string]bool)
	var actions []string

	for _, logLine := range solanaPayload.Result.Value.Logs {
		// Extract program invocations
		if strings.HasPrefix(logLine, "Program ") && strings.Contains(logLine, " invoke") {
			parts := strings.Split(logLine, " ")
			if len(parts) >= 2 {
				programID := parts[1]
				programs[programID] = true
			}
		}

		// Extract interesting log messages
		if strings.Contains(logLine, "Program log:") {
			msg := strings.TrimPrefix(logLine, "Program log: ")

			// Detect specific actions
			if strings.Contains(msg, "Transfer") {
				actions = append(actions, "💸 Token Transfer")
			}
			if strings.Contains(msg, "Swap") || strings.Contains(msg, "swap") {
				actions = append(actions, "🔄 Swap")
			}
			if strings.Contains(msg, "Price updated") {
				actions = append(actions, "📊 Price Oracle Update")
			}
			if strings.Contains(msg, "Mint") || strings.Contains(msg, "mint") {
				actions = append(actions, "🪙 Mint")
			}
			if strings.Contains(msg, "Burn") || strings.Contains(msg, "burn") {
				actions = append(actions, "🔥 Burn")
			}
		}
	}

	// Log programs involved
	log(1, "📋 Programs:")
	for programID := range programs {
		name, known := programNames[programID]
		if known {
			log(1, fmt.Sprintf("   • %s", name))
		} else {
			log(1, fmt.Sprintf("   • %s", truncate(programID, 16)))
		}
	}

	// Log detected actions
	if len(actions) > 0 {
		// Dedupe actions
		seen := make(map[string]bool)
		log(1, "⚡ Actions:")
		for _, action := range actions {
			if !seen[action] {
				log(1, fmt.Sprintf("   %s", action))
				seen[action] = true
			}
		}
	}

	log(1, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	// Build output
	output := map[string]interface{}{
		"status":    "success",
		"slot":      event.BlockNum,
		"signature": event.TxHash,
		"programs":  len(programs),
	}
	outputBytes, _ := json.Marshal(output)
	setOutput(string(outputBytes))
}

func truncate(s string, n int) string {
	if len(s) <= n*2+3 {
		return s
	}
	return s[:n] + "..." + s[len(s)-n:]
}
