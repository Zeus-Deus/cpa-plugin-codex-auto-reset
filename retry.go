package main

// retryDecision is what the FSM should do after a consume attempt.
type retryDecision int

const (
	// decisionSuccess: code=reset or already_redeemed -> proceed to VERIFYING.
	decisionSuccess retryDecision = iota
	// decisionRetry: transient failure (5xx, network, timeout, 401/403/408/
	// 409/425/429) -> retry the SAME redeem_request_id after retry_interval, so
	// a request that did land server-side is deduplicated (already_redeemed).
	decisionRetry
	// decisionNothingToReset: the server refused because no covered allowance
	// is low enough to restore. The credit is NOT consumed. Automatic cycles
	// re-confirm the same pinned credit after retry_interval and try again
	// with a NEW redeem_request_id (the old one may be bound to this answer);
	// manual resets stop.
	decisionNothingToReset
	// decisionStop: terminal (no_credit, permanent 4xx, unknown code) -> DONE.
	decisionStop
)

// classifyConsume maps a consume response (or error) to the FSM's next action.
//   - err == nil, code in {reset, already_redeemed} -> Success
//   - err == nil, code == nothing_to_reset         -> NothingToReset
//   - err == nil, code == no_credit                -> Stop
//   - err == nil, unknown code                     -> Stop (don't hammer)
//   - err is a permanent 4xx                       -> Stop
//   - any other error                              -> Retry (transient)
func classifyConsume(resp ConsumeResponse, err error) retryDecision {
	if err != nil {
		if IsPermanentClientError(err) {
			return decisionStop
		}
		return decisionRetry
	}
	switch resp.Code {
	case ConsumeCodeReset, ConsumeCodeAlreadyRedeemed:
		return decisionSuccess
	case ConsumeCodeNothingToReset:
		return decisionNothingToReset
	default:
		return decisionStop
	}
}
