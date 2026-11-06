package main

import (
	"testing"
	"time"
)

func TestConstants_MatchSpec(t *testing.T) {
	if usageEndpoint != "https://chatgpt.com/backend-api/wham/usage" {
		t.Fatalf("usageEndpoint = %q", usageEndpoint)
	}
	if resetCreditsEndpoint != "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits" {
		t.Fatalf("resetCreditsEndpoint = %q", resetCreditsEndpoint)
	}
	if consumeEndpoint != "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits/consume" {
		t.Fatalf("consumeEndpoint = %q", consumeEndpoint)
	}
	if codexUserAgent != "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal" {
		t.Fatalf("codexUserAgent = %q", codexUserAgent)
	}
	if postResetVerifyDelay != 1*time.Minute {
		t.Fatalf("postResetVerifyDelay = %v", postResetVerifyDelay)
	}
	if manualMaxRetries != 5 {
		t.Fatalf("manualMaxRetries = %d", manualMaxRetries)
	}
	if patrolErrorRetryInterval != 30*time.Minute {
		t.Fatalf("patrolErrorRetryInterval = %v", patrolErrorRetryInterval)
	}
	if maxLogEntries != 1000 {
		t.Fatalf("maxLogEntries = %d", maxLogEntries)
	}
	if logRetention != 7*24*time.Hour {
		t.Fatalf("logRetention = %v", logRetention)
	}
}
