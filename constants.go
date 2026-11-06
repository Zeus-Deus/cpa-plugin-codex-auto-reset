package main

import "time"

// OpenAI endpoint URLs — hardcoded per spec §2.1, never user-configurable.
// These full-URL constants are the authoritative documented contract; the
// OpenAI client also keeps *Path-suffixed constants for test BaseURL override.
const (
	usageEndpoint        = "https://chatgpt.com/backend-api/wham/usage"
	resetCreditsEndpoint = "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits"
	consumeEndpoint      = "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits/consume"
)

// codexUserAgent mirrors the value used by Cli-Proxy-API-Management-Center
// (spec §2.2). The codex_cli_rs/<version> prefix is the load-bearing part.
// Operators can override it with config_yaml `user_agent` (see userAgent()).
const codexUserAgent = "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal"

// Hardcoded scheduling/log parameters (spec §7.2). None are user-tunable; the
// retry cadence is the user-tunable retry_interval (config.go).
const (
	postResetVerifyDelay = 1 * time.Minute
	maxLogEntries        = 1000
	logRetention         = 7 * 24 * time.Hour
	// credentialRetryInterval is how soon an enabled account is retried after
	// its credentials fail to load (CPA auth file not ready at startup, token
	// mid-refresh, or operator re-login). Previously a failed initial load
	// left the account out of the FSM map forever, and a failure while running
	// waited a full refresh_interval (default 12h).
	credentialRetryInterval = 30 * time.Minute
	// wildcardResolveInterval throttles how often the worker re-queries the
	// host for the Codex account set while the all-accounts wildcard ("*") is
	// enabled. Without it the idle loop (100ms ticks when nothing is
	// scheduled) would hammer the host ABI.
	wildcardResolveInterval = 1 * time.Minute
)

// manualMaxRetries caps transient-error retries of a manual Reset (which may
// target a credit with no expiry, so expiry cannot bound it). Automatic cycles
// are bounded by the pinned credit's expiry instead.
const manualMaxRetries = 5

// patrolErrorRetryInterval caps how long a failed patrol or pre-reset
// confirmation waits before trying again, so one network blip near a credit's
// trigger time cannot push the next look past its expiry.
const patrolErrorRetryInterval = 30 * time.Minute
