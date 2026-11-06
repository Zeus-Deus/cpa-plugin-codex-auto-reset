package main

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ResetClient is the subset of OpenAIClient that the FSM needs. Defined here
// (next to the consumer) rather than in openai_client.go so the FSM is the
// dependency authority. Tests inject a fake; production injects *OpenAIClient.
type ResetClient interface {
	ListCredits(creds CodexCredentials) (Snapshot, error)
	GetUsage(creds CodexCredentials) (Snapshot, error)
	Consume(creds CodexCredentials, redeemRequestID, creditID string) (ConsumeResponse, error)
}

// LogFn is the FSM's logging callback. The worker wires it to the log ring.
type LogFn func(level, scope string, state State, msg, nextAction string, nextAt *time.Time, details map[string]any)

// AccountFSM is one account's state machine (spec §4). Deterministic given
// inputs; the simulation in sim/engine.go already validated the transitions.
//
// Concurrency: the worker goroutine drives Step(). External readers (worker's
// EachFSM, management handlers) may call State()/NextWake()/SetNextWake()/
// ForceConfirm() from other goroutines; all field access is guarded by mu.
type AccountFSM struct {
	AuthID string
	Creds  CodexCredentials
	Cfg    Config
	Client ResetClient
	Now    func() time.Time
	Logf   LogFn
	// Halted, when set (by the worker), reports that the owning worker was
	// stopped; no consume is sent after that (its replacement takes over).
	Halted func() bool
	// PausedFn, when set (by the worker), is the live pause switch. It is
	// read without the FSM lock's help, so pausing never waits for an
	// in-flight HTTP step. Nil falls back to Cfg.Paused (tests).
	PausedFn func() bool

	mu                sync.Mutex
	state             State
	attempt           ResetAttempt
	nextWake          time.Time
	verifyDue         time.Time
	scheduledCreditID string
	// scheduledExpiresAt is the pinned credit's expiry as seen when it was
	// armed (refreshed on every confirmation). It bounds automatic retries.
	scheduledExpiresAt time.Time
	manualReset        bool
	// testPostStep, when non-nil, runs inside StepAndSettle after the tick
	// but BEFORE the settle decision, with mu still held. Tests use it to
	// prove that a concurrent ForceConfirm/TriggerCheck cannot interleave
	// into the (old, racy) gap between step and schedule write. Production
	// leaves it nil.
	testPostStep func()
	// pendingHistory is set in stepVERIFYING with the reset result. NOT cleared
	// by Reset() so the worker can collect it after the cycle ends. The worker
	// sets it back to nil after collecting.
	pendingHistory *ResetRecord
	// lastSnapshot is the most recent Snapshot observed by any state step
	// (IDLE patrol, CONFIRMING pre-reset, VERIFYING post-reset). The worker
	// mirrors it into AccountRuntime so the management UI can show live
	// credit counts, weekly %, and next expiry without an extra API call.
	lastSnapshot Snapshot
}

// NewAccountFSM builds an FSM in StateIDLE, ready for its first Step().
func NewAccountFSM(authID string, creds CodexCredentials, cfg Config, client ResetClient, now func() time.Time, logf LogFn) *AccountFSM {
	return &AccountFSM{
		AuthID: authID, Creds: creds, Cfg: cfg, Client: client, Now: now, Logf: logf,
		state: StateIDLE,
	}
}

// State returns the current FSM state (thread-safe).
func (f *AccountFSM) State() State {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state
}

// NextWake returns the most recently scheduled wake time (thread-safe).
func (f *AccountFSM) NextWake() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.nextWake
}

// LastSnapshot returns the most recent Snapshot observed by any state step.
// Thread-safe.
func (f *AccountFSM) LastSnapshot() Snapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastSnapshot
}

// PullPatrolIfIdle schedules an immediate patrol if the FSM is IDLE (checked
// and written under one lock hold). Active cycles keep their schedule.
func (f *AccountFSM) PullPatrolIfIdle(now time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state == StateIDLE {
		f.nextWake = now
	}
}

// SetNextWake lets external callers (worker's TriggerCheck) override the
// scheduled wake. Thread-safe.
func (f *AccountFSM) SetNextWake(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextWake = t
}

// ForceConfirm requests a manual reset without interrupting consume or verify.
// Thread-safe; manual intent and its immediate wake are set together.
func (f *AccountFSM) ForceConfirm() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch f.state {
	case StateVERIFYING:
		return errCycleBusy
	case StateRESETTING:
		// An automatic attempt is pending (e.g. waiting for its retry, or
		// cancelled by a pause at its next step): take it over as a manual
		// reset of the SAME credit with the SAME request id, right now.
		if !f.manualReset {
			f.manualReset = true
			f.attempt.Attempt = 0 // fresh manual retry budget; same key and target
			f.attempt.NextRetryAt = time.Time{}
			f.nextWake = f.now()
		}
		return nil
	}
	f.scheduledCreditID = ""
	f.scheduledExpiresAt = time.Time{}
	f.manualReset = true
	f.state = StateCONFIRMING
	f.nextWake = f.now()
	return nil
}

// errCycleBusy: a reset was just sent and is being verified.
var errCycleBusy = fmt.Errorf("a reset is being verified for this account; try again in a minute")

// paused reports the live automatic-redeem pause switch.
func (f *AccountFSM) paused() bool {
	if f.PausedFn != nil {
		return f.PausedFn()
	}
	return f.Cfg.Paused
}

// Step executes one FSM tick at f.Now() and returns the next scheduled wake.
// Returns zero time when the cycle terminated (DONE). The automatic flow
// mirrors sim/engine.go's step(), covering (R, L) relations and edge cases.
//
// Step takes the FSM mutex for the whole tick; HTTP calls happen under lock,
// which is fine because the FSM is driven by a single worker goroutine (no
// other caller Step()s the same FSM).
//
// Step is the raw tick surface used by tests and simulations. The worker
// drives FSMs via StepAndSettle, which additionally completes the DONE→IDLE
// transition atomically (see its comment for why that must not be split).
func (f *AccountFSM) Step() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stepLocked()
}

func (f *AccountFSM) stepLocked() time.Time {
	switch f.state {
	case StateIDLE:
		return f.stepIDLE()
	case StateARMED:
		return f.stepARMED()
	case StateCONFIRMING:
		return f.stepCONFIRMING()
	case StateRESETTING:
		return f.stepRESETTING()
	case StateVERIFYING:
		return f.stepVERIFYING()
	case StateDONE:
		return time.Time{}
	}
	return time.Time{}
}

// StepAndSettle executes one tick and, under the same mutex hold, settles the
// post-tick schedule: when the cycle terminated (DONE) it performs the
// DONE→IDLE reset (clearing the pinned target and manual intent) and
// schedules the next patrol at now+refresh_interval; otherwise it records the
// wake the tick returned as nextWake. Returns the settled wake.
//
// Step + settle in one critical section closes the strand race where a
// ForceConfirm or TriggerCheck landing between the worker's Step() return and
// its follow-up Reset()/SetNextWake() could be overwritten with a zero wake —
// leaving a CONFIRMING FSM that soonest() skips forever. Manual triggers take
// the same mutex, so they are now ordered entirely before or after the settle.
func (f *AccountFSM) StepAndSettle() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	wake := f.stepLocked()
	if f.testPostStep != nil {
		f.testPostStep() // test-only interleaving hook; mu is still held
	}
	if wake.IsZero() && f.state == StateDONE {
		f.resetLocked()
		// Patrol again soon (not after a full refresh_interval): another
		// credit may already be close to expiry.
		wake = f.now().Add(f.retryInterval())
	}
	f.nextWake = wake
	return wake
}

func (f *AccountFSM) now() time.Time {
	if f.Now != nil {
		return f.Now()
	}
	return time.Now()
}

// triggerTime returns T = E - L (the moment the trigger window opens).
func (f *AccountFSM) triggerTime(c Credit) time.Time { return c.ExpiresAt.Add(-f.Cfg.TriggerLeadTime) }

// armThreshold returns M = T - R (the moment we enter ARMED).
func (f *AccountFSM) armThreshold(c Credit) time.Time {
	return f.triggerTime(c).Add(-f.Cfg.RefreshInterval)
}

func (f *AccountFSM) log(level, msg, nextAction string, nextAt *time.Time, details map[string]any) {
	if f.Logf != nil {
		f.Logf(level, f.AuthID, f.state, msg, nextAction, nextAt, details)
	}
}

// retryInterval is the configured retry cadence; a zero/negative value (an
// unnormalized Config, e.g. in tests) falls back to the default.
func (f *AccountFSM) retryInterval() time.Duration {
	if f.Cfg.RetryInterval <= 0 {
		return defaultRetryInterval
	}
	return f.Cfg.RetryInterval
}

// patrolRetryAt is when a failed patrol/confirmation looks again.
func (f *AccountFSM) patrolRetryAt(now time.Time) time.Time {
	return now.Add(f.retryInterval())
}

func (f *AccountFSM) stepIDLE() time.Time {
	now := f.now()
	snap, err := f.Client.ListCredits(f.Creds)
	if err != nil {
		next := f.patrolRetryAt(now)
		f.nextWake = next
		f.log("warn", fmt.Sprintf("list failed: %v", err), "retry patrol", &next, nil)
		return next
	}
	f.lastSnapshot = snap // mirror into the UI-facing snapshot
	// Also fetch weekly % so the UI card can display it. The /rate-limit-
	// reset-credits endpoint doesn't return weekly %, so we need /usage.
	if usage, err := f.Client.GetUsage(f.Creds); err == nil {
		snap.WeeklyPct = usage.WeeklyPct
		f.lastSnapshot = snap
	}
	avail := availableCreditsSorted(snap.Credits, now)
	if len(avail) == 0 {
		next := now.Add(f.Cfg.RefreshInterval)
		f.nextWake = next
		f.log("info", "no available credits", "patrol", &next, nil)
		return next
	}
	// Only credits WITH an expiry are ever scheduled automatically. Sorting
	// puts never-expiring credits last, so avail[0] has the soonest expiry.
	target := avail[0]
	if target.ExpiresAt.IsZero() {
		next := now.Add(f.Cfg.RefreshInterval)
		f.nextWake = next
		f.log("info", "no expiring credits to schedule", "patrol", &next, nil)
		return next
	}
	M := f.armThreshold(target)
	T := f.triggerTime(target)
	if now.Before(M) {
		next := now.Add(f.Cfg.RefreshInterval)
		f.nextWake = next
		f.log("info",
			fmt.Sprintf("credit %s expires in %s; not near trigger window (redeems %s before expiry)", shortID(target.ID), target.ExpiresAt.Sub(now).Round(time.Hour), formatDuration(f.Cfg.TriggerLeadTime)),
			"patrol", &next,
			map[string]any{"credit_id": target.ID, "expires_at": target.ExpiresAt})
		return next
	}
	if f.paused() {
		// Paused: report what would happen, never arm. Resuming triggers an
		// immediate patrol.
		next := now.Add(f.Cfg.RefreshInterval)
		f.nextWake = next
		f.log("warn",
			fmt.Sprintf("automatic redeem is paused: credit %s expires at %s and will not be redeemed automatically", shortID(target.ID), target.ExpiresAt.Format(time.RFC3339)),
			"patrol (paused)", &next,
			map[string]any{"credit_id": target.ID, "expires_at": target.ExpiresAt, "T": T})
		return next
	}
	// now >= M -> ARMED. Sleep to T (or now if T already passed: late start,
	// spec §4.2.1). ARMED does NO extra GETs; it just transitions on wake.
	f.scheduledCreditID = target.ID
	f.scheduledExpiresAt = target.ExpiresAt
	f.state = StateARMED
	wake := T
	if !now.Before(T) {
		wake = now
	}
	f.nextWake = wake
	f.log("warn",
		fmt.Sprintf("credit %s armed; will reset at %s (expiry in %s)", shortID(target.ID), T.Format(time.RFC3339), target.ExpiresAt.Sub(now).Round(time.Minute)),
		"sleep to trigger time", &wake,
		map[string]any{"credit_id": target.ID, "T": T, "expires_at": target.ExpiresAt})
	return wake
}

func (f *AccountFSM) stepARMED() time.Time {
	// A Check may wake ARMED early; CONFIRMING revalidates the trigger time.
	// Zero extra GETs in this state (revised design).
	f.state = StateCONFIRMING
	f.log("info", "armed wake, confirming target and trigger time", "confirm before reset", nil, nil)
	return f.now() // re-enter immediately
}

// abandonAuto ends an automatic cycle without sending (another) consume.
func (f *AccountFSM) abandonAuto(level, msg string) time.Time {
	f.state = StateDONE
	f.log(level, msg, "abandon", nil, map[string]any{"credit_id": f.scheduledCreditID})
	return time.Time{}
}

// retryAutoConfirm schedules another confirmation of the SAME pinned credit
// after retry_interval, or abandons when that would land at/after its expiry.
func (f *AccountFSM) retryAutoConfirm(now time.Time, why string) time.Time {
	next := now.Add(f.retryInterval())
	if f.scheduledExpiresAt.IsZero() || !next.Before(f.scheduledExpiresAt) {
		return f.abandonAuto("error", fmt.Sprintf("%s; credit %s expires at %s, before the next retry: giving up", why, shortID(f.scheduledCreditID), f.scheduledExpiresAt.Format(time.RFC3339)))
	}
	f.state = StateARMED
	f.attempt = ResetAttempt{}
	f.nextWake = next
	f.log("warn", fmt.Sprintf("%s; retrying credit %s", why, shortID(f.scheduledCreditID)), "retry confirm", &next,
		map[string]any{"credit_id": f.scheduledCreditID, "expires_at": f.scheduledExpiresAt})
	return next
}

func (f *AccountFSM) stepCONFIRMING() time.Time {
	now := f.now()
	if !f.manualReset && f.paused() {
		return f.abandonAuto("warn", "automatic redeem is paused; armed cycle cancelled before any reset request")
	}
	snap, err := f.Client.ListCredits(f.Creds)
	if err != nil {
		if f.manualReset {
			f.state = StateDONE
			f.log("error", fmt.Sprintf("confirm list failed: %v", err), "abandon", nil, nil)
			return time.Time{}
		}
		return f.retryAutoConfirm(now, fmt.Sprintf("confirm list failed: %v", err))
	}
	avail := availableCreditsSorted(snap.Credits, now)
	var target Credit
	if f.manualReset {
		if len(avail) > 0 {
			target = avail[0]
		}
	} else if len(avail) > 0 && !avail[0].ExpiresAt.IsZero() {
		// Automatic: always the soonest-expiring available credit right now
		// (never-expiring ones sort last and are never chosen). If it differs
		// from the armed one (that one was redeemed elsewhere, or a sooner one
		// appeared), re-pin; the trigger-time check below applies to it. A
		// fresh request id is minted below, so nothing ambiguous is dropped.
		target = avail[0]
		if target.ID != f.scheduledCreditID {
			f.log("warn", fmt.Sprintf("target changed from %s to %s (now the soonest-expiring credit)", shortID(f.scheduledCreditID), shortID(target.ID)),
				"re-pin", nil, map[string]any{"from": f.scheduledCreditID, "to": target.ID, "expires_at": target.ExpiresAt})
		}
	}
	if target.ID == "" {
		f.state = StateDONE
		f.log("warn", "no available expiring credit before reset", "abandon", nil,
			map[string]any{"credit_id": f.scheduledCreditID})
		return time.Time{}
	}
	if !f.manualReset {
		f.scheduledCreditID = target.ID
		f.scheduledExpiresAt = target.ExpiresAt
		T := f.triggerTime(target)
		if now.Before(f.armThreshold(target)) {
			f.state = StateDONE
			f.log("info", fmt.Sprintf("credit %s is not near its trigger time any more", shortID(target.ID)), "patrol", nil,
				map[string]any{"credit_id": target.ID, "expires_at": target.ExpiresAt})
			return time.Time{}
		}
		if now.Before(T) {
			f.state = StateARMED
			f.nextWake = T
			f.log("info", "target credit not yet due; re-armed", "sleep to trigger time", &T,
				map[string]any{"credit_id": target.ID, "T": T})
			return T
		}
	}
	// Fetch usage separately so PreSnapshot.WeeklyPct carries a real value.
	// The reset-credits endpoint does NOT return weekly % (parseResetCreditsResponse
	// sets WeeklyPct=-1); without this call, the triple-check's quota leg
	// (quotaUp) would be trivially true (any value > -1) and the safety
	// invariant would degrade to a two-leg check. Spec §4.2 mandates both calls.
	usage, err := f.Client.GetUsage(f.Creds)
	if err != nil {
		if f.manualReset {
			f.state = StateDONE
			f.log("error", fmt.Sprintf("confirm usage failed: %v", err), "abandon", nil, nil)
			return time.Time{}
		}
		return f.retryAutoConfirm(now, fmt.Sprintf("confirm usage failed: %v", err))
	}
	snap.WeeklyPct = usage.WeeklyPct
	f.attempt = ResetAttempt{
		RedeemRequestID: uuid.NewString(),
		TargetCreditID:  target.ID,
		TargetExpiresAt: target.ExpiresAt,
		PreSnapshot:     snap,
		StartedAt:       now,
		Attempt:         0,
	}
	f.state = StateRESETTING
	f.log("info",
		fmt.Sprintf("confirmed target credit %s (pre: credits=%d, weekly=%d%%); sending reset request",
			shortID(target.ID), snap.AvailableCount, snap.WeeklyPct),
		"send reset request", nil,
		map[string]any{
			"credit_id":         target.ID,
			"pre_credits":       snap.AvailableCount,
			"pre_weekly_pct":    snap.WeeklyPct,
			"target_expires_at": target.ExpiresAt,
			"redeem_request_id": f.attempt.RedeemRequestID,
			"manual":            f.manualReset,
		})
	return f.now()
}

func (f *AccountFSM) stepRESETTING() time.Time {
	now := f.now()
	auto := !f.manualReset
	if f.attempt.Attempt > 0 {
		// A retry: re-list first. If the pinned credit is gone, an earlier
		// (ambiguous) attempt probably succeeded: verify instead of sending
		// again. Otherwise refresh its expiry for the window checks below.
		snap, err := f.Client.ListCredits(f.Creds)
		if err != nil {
			return f.scheduleConsumeRetry(now, auto, fmt.Errorf("re-list before retry: %w", err))
		}
		var found *Credit
		for _, c := range availableCreditsSorted(snap.Credits, now) {
			if c.ID == f.attempt.TargetCreditID {
				c := c
				found = &c
				break
			}
		}
		if found == nil {
			// Give the server the usual settle time (a credit can briefly be
			// "redeeming") before the deduction check.
			f.state = StateVERIFYING
			f.verifyDue = now.Add(postResetVerifyDelay)
			f.nextWake = f.verifyDue
			f.log("warn", fmt.Sprintf("credit %s is no longer available before retry; verifying the earlier attempt instead of resending", shortID(f.attempt.TargetCreditID)),
				"verify", timePtr(f.verifyDue), map[string]any{"redeem_request_id": f.attempt.RedeemRequestID})
			return f.verifyDue
		}
		if auto {
			if found.ExpiresAt.IsZero() {
				return f.abandonAuto("info", "target credit no longer expires; automatic retry cancelled")
			}
			f.attempt.TargetExpiresAt = found.ExpiresAt
			f.scheduledExpiresAt = found.ExpiresAt
		}
		now = f.now() // the re-list took time; check the window against fresh time
	}
	if auto {
		// Defense in depth: an automatic consume is only ever sent for the
		// pinned credit, inside [expiry - lead, expiry), and not while paused.
		switch {
		case f.attempt.TargetCreditID == "" || f.attempt.TargetCreditID != f.scheduledCreditID:
			return f.abandonAuto("error", "internal: reset target does not match the armed credit; no request sent")
		case f.paused():
			return f.abandonAuto("warn", "automatic redeem is paused; no reset request sent")
		case f.attempt.TargetExpiresAt.IsZero() || !now.Before(f.attempt.TargetExpiresAt):
			return f.abandonAuto("error", fmt.Sprintf("credit %s expired before the reset request could be sent", shortID(f.attempt.TargetCreditID)))
		case now.Before(f.attempt.TargetExpiresAt.Add(-f.Cfg.TriggerLeadTime)):
			// Its expiry moved later: not due any more. Re-arm (new id later),
			// or end the cycle if it is no longer within one patrol of T.
			T := f.attempt.TargetExpiresAt.Add(-f.Cfg.TriggerLeadTime)
			if now.Before(T.Add(-f.Cfg.RefreshInterval)) {
				return f.abandonAuto("info", "target credit's expiry moved far out; no reset request sent, normal patrols resume")
			}
			f.state = StateARMED
			f.attempt = ResetAttempt{}
			f.nextWake = T
			f.log("info", "target credit's expiry moved later; re-armed, no reset request sent", "sleep to trigger time", &T,
				map[string]any{"credit_id": f.scheduledCreditID, "T": T})
			return T
		}
	}
	// Last check right before the POST: a replaced (stopped) worker must not
	// send anything; its successor re-checks from scratch.
	if f.Halted != nil && f.Halted() {
		f.log("info", "worker stopping; reset request not sent (the restarted worker re-checks)", "none", nil, nil)
		return time.Time{}
	}
	resp, err := f.Client.Consume(f.Creds, f.attempt.RedeemRequestID, f.attempt.TargetCreditID)
	switch classifyConsume(resp, err) {
	case decisionSuccess:
		f.state = StateVERIFYING
		f.verifyDue = now.Add(postResetVerifyDelay)
		f.nextWake = f.verifyDue
		f.log("info",
			fmt.Sprintf("reset request accepted (code=%s, windows_reset=%d); will verify in %v",
				resp.Code, resp.WindowsReset, postResetVerifyDelay),
			fmt.Sprintf("verify in %v", postResetVerifyDelay), timePtr(f.verifyDue),
			map[string]any{
				"redeem_request_id": f.attempt.RedeemRequestID,
				"credit_id":         f.attempt.TargetCreditID,
				"consume_code":      resp.Code,
				"windows_reset":     resp.WindowsReset,
			})
		return f.verifyDue
	case decisionNothingToReset:
		msg := fmt.Sprintf("OpenAI answered nothing_to_reset for credit %s (no covered limit is low enough to restore); the credit was NOT used",
			shortID(f.attempt.TargetCreditID))
		if auto {
			return f.retryAutoConfirm(now, msg)
		}
		f.state = StateDONE
		f.log("warn", msg, "abandon", nil, map[string]any{"redeem_request_id": f.attempt.RedeemRequestID})
		return time.Time{}
	case decisionStop:
		f.state = StateDONE
		msg := fmt.Sprintf("reset stopped: server returned %s", resp.Code)
		if err != nil {
			msg = fmt.Sprintf("reset stopped: %v", err)
		}
		f.log("warn", msg, "abandon", nil, map[string]any{"redeem_request_id": f.attempt.RedeemRequestID})
		return time.Time{}
	case decisionRetry:
		return f.scheduleConsumeRetry(now, auto, err)
	}
	return time.Time{}
}

// scheduleConsumeRetry keeps the SAME redeem_request_id and target and retries
// after retry_interval: automatic cycles until the credit's expiry, manual
// resets at most manualMaxRetries times.
func (f *AccountFSM) scheduleConsumeRetry(now time.Time, auto bool, err error) time.Time {
	f.attempt.Attempt++
	next := now.Add(f.retryInterval())
	if auto && !next.Before(f.attempt.TargetExpiresAt) {
		f.state = StateDONE
		f.log("error",
			fmt.Sprintf("reset failed (attempt %d): %v; credit expires before the next retry: giving up", f.attempt.Attempt, err),
			"abandon", nil,
			map[string]any{"redeem_request_id": f.attempt.RedeemRequestID})
		return time.Time{}
	}
	if !auto && f.attempt.Attempt > manualMaxRetries {
		f.state = StateDONE
		f.log("error",
			fmt.Sprintf("retries exhausted after %d attempts; last error: %v", f.attempt.Attempt-1, err),
			"abandon", nil,
			map[string]any{"redeem_request_id": f.attempt.RedeemRequestID})
		return time.Time{}
	}
	f.attempt.NextRetryAt = next
	f.nextWake = next
	f.log("warn",
		fmt.Sprintf("reset failed (attempt %d): %v; retrying with same idempotency key", f.attempt.Attempt, err),
		fmt.Sprintf("retry in %v", f.retryInterval()), timePtr(next),
		map[string]any{"redeem_request_id": f.attempt.RedeemRequestID, "attempt": f.attempt.Attempt})
	return next
}

func (f *AccountFSM) stepVERIFYING() time.Time {
	credits, err := f.Client.ListCredits(f.Creds)
	if err != nil {
		f.state = StateDONE
		f.log("error", fmt.Sprintf("verification failed: %v", err), "abandon", nil,
			map[string]any{"redeem_request_id": f.attempt.RedeemRequestID, "target_credit_id": f.attempt.TargetCreditID})
		return time.Time{}
	}
	usage, err := f.Client.GetUsage(f.Creds)
	if err != nil {
		f.state = StateDONE
		f.log("error", fmt.Sprintf("verification failed: %v", err), "abandon", nil,
			map[string]any{"redeem_request_id": f.attempt.RedeemRequestID, "target_credit_id": f.attempt.TargetCreditID})
		return time.Time{}
	}
	// Update lastSnapshot with post-reset data so the UI shows the new state.
	credits.WeeklyPct = usage.WeeklyPct
	f.lastSnapshot = credits

	// Triple deduction check (spec §4.3 invariant 3).
	targetGone := true
	for _, c := range credits.Credits {
		if c.ID == f.attempt.TargetCreditID && c.Status == "available" {
			targetGone = false
			break
		}
	}
	countDown1 := credits.AvailableCount == f.attempt.PreSnapshot.AvailableCount-1
	// Unknown (-1) post-reset quota never counts as recovered.
	// Both readings must be known: an unknown (-1) baseline would make any
	// known value look like an increase.
	quotaUp := usage.WeeklyPct >= 0 && f.attempt.PreSnapshot.WeeklyPct >= 0 &&
		(usage.WeeklyPct >= 100 || usage.WeeklyPct > f.attempt.PreSnapshot.WeeklyPct)

	f.state = StateDONE
	// Build detailed details map with full before/after snapshot.
	details := map[string]any{
		"credit_id":         f.attempt.TargetCreditID,
		"redeem_request_id": f.attempt.RedeemRequestID,
		"pre_credits":       f.attempt.PreSnapshot.AvailableCount,
		"post_credits":      credits.AvailableCount,
		"pre_weekly_pct":    f.attempt.PreSnapshot.WeeklyPct,
		"post_weekly_pct":   usage.WeeklyPct,
		"target_gone":       targetGone,
		"count_down_1":      countDown1,
		"quota_recovered":   quotaUp,
	}

	success := targetGone && countDown1 && quotaUp

	switch {
	case success:
		f.log("info",
			fmt.Sprintf("reset verified OK: credits %d→%d, weekly %d%%→%d%%, target %s consumed",
				f.attempt.PreSnapshot.AvailableCount, credits.AvailableCount,
				f.attempt.PreSnapshot.WeeklyPct, usage.WeeklyPct,
				shortID(f.attempt.TargetCreditID)),
			"cycle complete", nil, details)
	case targetGone && countDown1:
		f.log("warn",
			fmt.Sprintf("reset PARTIAL: credits %d→%d ok but weekly %d%%→%d%% (delayed recovery?)",
				f.attempt.PreSnapshot.AvailableCount, credits.AvailableCount,
				f.attempt.PreSnapshot.WeeklyPct, usage.WeeklyPct),
			"cycle complete", nil, details)
	default:
		f.log("error",
			fmt.Sprintf("reset MISMATCH: target_gone=%v count_down_1=%v quota_recovered=%v; no compensating request (patrols continue)",
				targetGone, countDown1, quotaUp),
			"abandon", nil, details)
	}

	// Capture the reset result for history. NOT cleared by Reset().
	f.pendingHistory = &ResetRecord{
		Timestamp:     f.now(),
		AuthID:        f.AuthID,
		CreditID:      f.attempt.TargetCreditID,
		Success:       success,
		PreWeeklyPct:  f.attempt.PreSnapshot.WeeklyPct,
		PostWeeklyPct: usage.WeeklyPct,
		PreCredits:    f.attempt.PreSnapshot.AvailableCount,
		PostCredits:   credits.AvailableCount,
	}

	return time.Time{}
}

// Reset returns a DONE FSM to IDLE for the next patrol cycle. Called by the
// worker after a cycle completes. Thread-safe. Does NOT clear pendingHistory
// — the worker collects it separately via TakePendingHistory().
func (f *AccountFSM) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resetLocked()
}

// resetLocked is Reset's body for callers already holding mu (StepAndSettle).
func (f *AccountFSM) resetLocked() {
	f.state = StateIDLE
	f.attempt = ResetAttempt{}
	f.verifyDue = time.Time{}
	f.nextWake = time.Time{}
	f.scheduledCreditID = ""
	f.scheduledExpiresAt = time.Time{}
	f.manualReset = false
}

// TakePendingHistory returns the reset result captured in stepVERIFYING and
// clears it. Returns nil if no pending history. Thread-safe.
func (f *AccountFSM) TakePendingHistory() *ResetRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec := f.pendingHistory
	f.pendingHistory = nil
	return rec
}

// timePtr returns a pointer to a copy (log entries must not alias FSM fields
// that later steps mutate).
func timePtr(t time.Time) *time.Time { return &t }

// shortID truncates a long credit/redeem ID for readable log messages.
// "RateLimitResetCredit_d7087f83469c819182a87d5916512c9c" -> "d7087f83…"
func shortID(id string) string {
	const prefix = "RateLimitResetCredit_"
	s := strings.TrimPrefix(id, prefix)
	if len(s) > 10 {
		return s[:8] + "…"
	}
	return s
}
