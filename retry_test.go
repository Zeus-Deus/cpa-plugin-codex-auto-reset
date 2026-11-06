package main

import (
	"errors"
	"testing"
)

func TestClassifyConsume_SuccessCodes(t *testing.T) {
	cases := []struct {
		name string
		resp ConsumeResponse
		want retryDecision
	}{
		{"reset", ConsumeResponse{Code: ConsumeCodeReset}, decisionSuccess},
		{"already_redeemed", ConsumeResponse{Code: ConsumeCodeAlreadyRedeemed}, decisionSuccess},
		{"no_credit", ConsumeResponse{Code: ConsumeCodeNoCredit}, decisionStop},
		{"nothing_to_reset", ConsumeResponse{Code: ConsumeCodeNothingToReset}, decisionNothingToReset},
		{"unknown_code", ConsumeResponse{Code: "garbage"}, decisionStop},
		{"empty_code", ConsumeResponse{}, decisionStop},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyConsume(tc.resp, nil); got != tc.want {
				t.Fatalf("classify %s => %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

// sentinel error types for the retry classification tests.
type netErr struct{}

func (netErr) Error() string { return "dial tcp: connection refused" }

func TestClassifyConsume_ErrorClassification(t *testing.T) {
	// Permanent 4xx -> hard stop. 401/403/408/409/425/429 (token mid-refresh,
	// edge challenge, rate limit) -> retry after retry_interval: credentials
	// are reloaded before each attempt, and retries are bounded by the
	// credit's expiry (automatic) or manualMaxRetries (manual).
	// 5xx and network errors -> retry.
	// Plain (non-HTTPStatusError) errors are treated as retryable (network,
	// timeout, DNS, etc.).
	cases := []struct {
		name string
		err  error
		want retryDecision
	}{
		{"400_bad_request", &HTTPStatusError{Code: 400}, decisionStop},
		{"404_not_found", &HTTPStatusError{Code: 404}, decisionStop},
		{"422_unprocessable", &HTTPStatusError{Code: 422}, decisionStop},
		{"401_unauthorized", &HTTPStatusError{Code: 401}, decisionRetry},
		{"403_forbidden", &HTTPStatusError{Code: 403}, decisionRetry},
		{"429_rate_limited", &HTTPStatusError{Code: 429}, decisionRetry},
		{"5xx_server_error", &HTTPStatusError{Code: 502}, decisionRetry},
		{"5xx_gateway_timeout", &HTTPStatusError{Code: 504}, decisionRetry},
		{"plain_network_error", netErr{}, decisionRetry},
		{"wrapped_network_error", errors.New("wrapped: " + (netErr{}).Error()), decisionRetry},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyConsume(ConsumeResponse{}, tc.err); got != tc.want {
				t.Fatalf("classify err %s => %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}
