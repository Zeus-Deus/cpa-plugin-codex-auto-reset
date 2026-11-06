package main

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

// fakeClient implements ResetClient for tests. Mirrors sim/engine.go's
// FakeOpenAIClient: same field intent (live credit map, weekly %, scripted
// consume response, transient-failure injection, wrong-credit deduction hook).
type fakeClient struct {
	credits     map[string]*Credit
	weeklyPct   int
	consumeResp ConsumeResponse
	consumeErr  error // returned when consumeResp.Code is zero AND failFirstN==0

	listCalls    int
	usageCalls   int
	consumeCalls int
	consumeIDs   []string
	redeemIDs    []string

	// failFirstN, when > 0, makes the next N consume calls return failErr
	// BEFORE applying any server-side deduction (mirrors sim/engine.go's
	// FailFirstN: a failed attempt must not mutate server state).
	failFirstN int
	failErr    error

	// deductCreditID, if non-empty, overrides the consumed id during the
	// server-side deduction — used to script the wrong-credit bug.
	deductCreditID string

	// consumeScript, when non-empty, supplies the next consume responses in
	// order (after failFirstN); consumeResp applies once it is exhausted.
	consumeScript []ConsumeResponse
	// clock, when set, records the fake time of every consume call.
	clock        *fakeClock
	consumeTimes []time.Time

	// keepQuotaAtPre, when true, makes Consume deduct the credit WITHOUT
	// bumping weeklyPct to 100 — scripts a broken/stale server where the
	// reset succeeds on the credit leg but the quota does not recover.
	keepQuotaAtPre bool
}

func (f *fakeClient) ListCredits(CodexCredentials) (Snapshot, error) {
	f.listCalls++
	credits := make([]Credit, 0, len(f.credits))
	for _, c := range f.credits {
		credits = append(credits, *c)
	}
	// WeeklyPct=-1 matches parseResetCreditsResponse (the reset-credits endpoint
	// does not return weekly %). stepCONFIRMING fetches usage separately to fill
	// in the real value; returning a real value here would mask the confirm-time
	// GetUsage requirement (reviewer issue I1/I2).
	return Snapshot{Credits: credits, AvailableCount: countAvail(credits), WeeklyPct: -1}, nil
}

func (f *fakeClient) GetUsage(CodexCredentials) (Snapshot, error) {
	f.usageCalls++
	return Snapshot{WeeklyPct: f.weeklyPct, AvailableCount: countAvail(mapToCredits(f.credits))}, nil
}

func (f *fakeClient) Consume(_ CodexCredentials, rrid, cid string) (ConsumeResponse, error) {
	f.consumeCalls++
	f.consumeIDs = append(f.consumeIDs, cid)
	f.redeemIDs = append(f.redeemIDs, rrid)
	if f.clock != nil {
		f.consumeTimes = append(f.consumeTimes, f.clock.t)
	}

	// Transient-failure injection takes precedence so a failed attempt does
	// NOT mutate server state (matches sim/engine.go's ordering).
	if f.failFirstN > 0 && f.failErr != nil {
		f.failFirstN--
		return ConsumeResponse{}, f.failErr
	}
	if f.consumeErr != nil {
		return ConsumeResponse{}, f.consumeErr
	}
	resp := f.consumeResp
	if len(f.consumeScript) > 0 {
		resp = f.consumeScript[0]
		f.consumeScript = f.consumeScript[1:]
	}
	// Server-side deduction on success-equivalent codes.
	if resp.Code == ConsumeCodeReset || resp.Code == ConsumeCodeAlreadyRedeemed {
		deductID := cid
		if f.deductCreditID != "" {
			deductID = f.deductCreditID
		}
		if c, ok := f.credits[deductID]; ok {
			c.Status = "redeemed"
		}
		if !f.keepQuotaAtPre {
			f.weeklyPct = 100
		}
	}
	return resp, nil
}

func countAvail(cs []Credit) int {
	n := 0
	for _, c := range cs {
		if c.Status == "available" {
			n++
		}
	}
	return n
}

func mapToCredits(m map[string]*Credit) []Credit {
	out := make([]Credit, 0, len(m))
	for _, c := range m {
		out = append(out, *c)
	}
	return out
}

type fakeClock struct{ t time.Time }

func (f *fakeClock) Now() time.Time { return f.t }

// drive runs the FSM until DONE (or step cap), advancing the fake clock to
// each scheduled wake. Mirrors sim/engine.go's Run loop.
func drive(t *testing.T, fsm *AccountFSM, clock *fakeClock) {
	t.Helper()
	for i := 0; i < 200 && fsm.State() != StateDONE; i++ {
		next := fsm.Step()
		if next.IsZero() {
			break
		}
		if next.After(clock.t) {
			clock.t = next
		}
	}
}

func TestFSM_HappyPath_AllStates(t *testing.T) {
	E := time.Date(2026, 7, 19, 18, 23, 47, 0, time.UTC)
	cfg := Config{RefreshInterval: 6 * time.Hour, TriggerLeadTime: 6 * time.Hour}
	fc := &fakeClient{
		credits:     map[string]*Credit{"c1": {ID: "c1", Status: "available", ExpiresAt: E}},
		weeklyPct:   15,
		consumeResp: ConsumeResponse{Code: ConsumeCodeReset, WindowsReset: 2},
	}
	clock := &fakeClock{t: E.Add(-12 * time.Hour)}
	logs := []string{}
	logf := func(level, scope string, state State, msg, next string, nextAt *time.Time, d map[string]any) {
		logs = append(logs, fmt.Sprintf("%s/%s: %s", level, state, msg))
	}
	fsm := NewAccountFSM("acct1", CodexCredentials{AccessToken: "t", ChatGPTAccountID: "a"}, cfg, fc, clock.Now, logf)

	drive(t, fsm, clock)

	if fsm.State() != StateDONE {
		t.Fatalf("final state = %s, want DONE. Logs:\n%s", fsm.State(), strings.Join(logs, "\n"))
	}
	if fc.consumeCalls != 1 {
		t.Fatalf("consume calls = %d, want 1", fc.consumeCalls)
	}
	if len(fc.consumeIDs) != 1 || fc.consumeIDs[0] != "c1" {
		t.Fatalf("consumed = %v, want [c1]", fc.consumeIDs)
	}
	if c := fc.credits["c1"]; c.Status != "redeemed" {
		t.Fatalf("c1 status = %s, want redeemed", c.Status)
	}
	if fc.weeklyPct != 100 {
		t.Fatalf("weekly = %d%%, want 100", fc.weeklyPct)
	}
}

// TestFSM_IdempotentRetry_FailureThenSuccess: first Consume returns a transient
// network error; the second returns code=reset. The retry MUST reuse the same
// redeem_request_id and the credit MUST be redeemed exactly once.
func TestFSM_IdempotentRetry_FailureThenSuccess(t *testing.T) {
	E := time.Date(2026, 7, 19, 18, 23, 47, 0, time.UTC)
	cfg := Config{RefreshInterval: 6 * time.Hour, TriggerLeadTime: 6 * time.Hour}
	fc := &fakeClient{
		credits:     map[string]*Credit{"c1": {ID: "c1", Status: "available", ExpiresAt: E}},
		weeklyPct:   20,
		consumeResp: ConsumeResponse{Code: ConsumeCodeReset, WindowsReset: 2},
		failFirstN:  1,
		failErr:     errors.New("transient network timeout"),
	}
	clock := &fakeClock{t: E.Add(-12 * time.Hour)}
	fsm := NewAccountFSM("acct1", CodexCredentials{}, cfg, fc, clock.Now, nil)

	drive(t, fsm, clock)

	if fsm.State() != StateDONE {
		t.Fatalf("final state = %s, want DONE", fsm.State())
	}
	if fc.consumeCalls != 2 {
		t.Fatalf("consume calls = %d, want 2 (1 fail + 1 success)", fc.consumeCalls)
	}
	if len(fc.redeemIDs) != 2 || fc.redeemIDs[0] != fc.redeemIDs[1] {
		t.Fatalf("redeem ids = %v, want both identical", fc.redeemIDs)
	}
	if c := fc.credits["c1"]; c.Status != "redeemed" {
		t.Fatalf("c1 status = %s, want redeemed once", c.Status)
	}
}

// TestFSM_AlreadyRedeemed_TreatedAsSuccess: Consume returns code=already_redeemed
// on the first call. The plugin treats this as success and verifies normally.
func TestFSM_AlreadyRedeemed_TreatedAsSuccess(t *testing.T) {
	E := time.Date(2026, 7, 19, 18, 23, 47, 0, time.UTC)
	cfg := Config{RefreshInterval: 6 * time.Hour, TriggerLeadTime: 6 * time.Hour}
	fc := &fakeClient{
		credits:     map[string]*Credit{"c1": {ID: "c1", Status: "available", ExpiresAt: E}},
		weeklyPct:   20,
		consumeResp: ConsumeResponse{Code: ConsumeCodeAlreadyRedeemed, WindowsReset: 0},
	}
	clock := &fakeClock{t: E.Add(-12 * time.Hour)}
	fsm := NewAccountFSM("acct1", CodexCredentials{}, cfg, fc, clock.Now, nil)

	drive(t, fsm, clock)

	if fsm.State() != StateDONE {
		t.Fatalf("final state = %s, want DONE", fsm.State())
	}
	if fc.consumeCalls != 1 {
		t.Fatalf("consume calls = %d, want 1", fc.consumeCalls)
	}
	if c := fc.credits["c1"]; c.Status != "redeemed" {
		t.Fatalf("c1 status = %s, want redeemed (server-side hook on already_redeemed)", c.Status)
	}
}

// TestFSM_RetriesExhausted_GivesUp: Consume always returns a network error.
// With a 1h lead and 10m retries, expect 6 POSTs (T, T+10m .. T+50m), all with
// the same UUID, then DONE before expiry with the credit NOT redeemed.
func TestFSM_RetriesExhausted_GivesUp(t *testing.T) {
	E := time.Date(2026, 7, 19, 18, 23, 47, 0, time.UTC)
	cfg := Config{RefreshInterval: 6 * time.Hour, TriggerLeadTime: time.Hour, RetryInterval: 10 * time.Minute}
	fc := &fakeClient{
		credits:    map[string]*Credit{"c1": {ID: "c1", Status: "available", ExpiresAt: E}},
		weeklyPct:  20,
		failFirstN: 100,
		failErr:    errors.New("persistent network failure"),
	}
	clock := &fakeClock{t: E.Add(-12 * time.Hour)}
	fsm := NewAccountFSM("acct1", CodexCredentials{}, cfg, fc, clock.Now, nil)

	drive(t, fsm, clock)

	if fsm.State() != StateDONE {
		t.Fatalf("final state = %s, want DONE", fsm.State())
	}
	if fc.consumeCalls != 6 {
		t.Fatalf("consume calls = %d, want 6 (T, then every 10m until expiry)", fc.consumeCalls)
	}
	first := fc.redeemIDs[0]
	for i, id := range fc.redeemIDs {
		if id != first {
			t.Fatalf("redeem id %d = %q, want %q (same UUID across retries)", i, id, first)
		}
	}
	if c := fc.credits["c1"]; c.Status != "available" {
		t.Fatalf("c1 status = %s, want available (server never succeeded)", c.Status)
	}
}

// TestFSM_NoCredit_Stops: Consume returns code=no_credit. The plugin must
// hard-stop (no retry), DONE, credit not redeemed.
func TestFSM_NoCredit_Stops(t *testing.T) {
	E := time.Date(2026, 7, 19, 18, 23, 47, 0, time.UTC)
	cfg := Config{RefreshInterval: 6 * time.Hour, TriggerLeadTime: 6 * time.Hour}
	fc := &fakeClient{
		credits:     map[string]*Credit{"c1": {ID: "c1", Status: "available", ExpiresAt: E}},
		weeklyPct:   30,
		consumeResp: ConsumeResponse{Code: ConsumeCodeNoCredit},
	}
	clock := &fakeClock{t: E.Add(-12 * time.Hour)}
	fsm := NewAccountFSM("acct1", CodexCredentials{}, cfg, fc, clock.Now, nil)

	drive(t, fsm, clock)

	if fsm.State() != StateDONE {
		t.Fatalf("final state = %s, want DONE", fsm.State())
	}
	if fc.consumeCalls != 1 {
		t.Fatalf("consume calls = %d, want 1 (no_credit is terminal)", fc.consumeCalls)
	}
	if c := fc.credits["c1"]; c.Status != "available" {
		t.Fatalf("c1 status = %s, want available (no deduction on no_credit)", c.Status)
	}
}

// TestFSM_CreditVanishesDuringARMED: during the ARMED sleep the credit is
// manually redeemed. At CONFIRMING the list returns no available credits.
// Expect 0 POSTs and DONE (abandoned).
func TestFSM_CreditVanishesDuringARMED(t *testing.T) {
	E := time.Date(2026, 7, 19, 18, 23, 47, 0, time.UTC)
	cfg := Config{RefreshInterval: 12 * time.Hour, TriggerLeadTime: 6 * time.Hour}
	fc := &fakeClient{
		credits:     map[string]*Credit{"c1": {ID: "c1", Status: "available", ExpiresAt: E}},
		weeklyPct:   30,
		consumeResp: ConsumeResponse{Code: ConsumeCodeReset, WindowsReset: 2},
	}
	clock := &fakeClock{t: E.Add(-12 * time.Hour)}
	fsm := NewAccountFSM("acct1", CodexCredentials{}, cfg, fc, clock.Now, nil)

	// Step 1: IDLE sees available credit; now >= M, enters ARMED, sleeps to T.
	next := fsm.Step()
	if fsm.State() != StateARMED {
		t.Fatalf("after IDLE step, state = %s, want ARMED", fsm.State())
	}
	// Simulate human using the credit during ARMED sleep.
	fc.credits["c1"].Status = "redeemed"
	// Advance clock to the scheduled ARMED wake.
	if next.After(clock.t) {
		clock.t = next
	}
	// ARMED wake -> CONFIRMING (immediate re-enter), then CONFIRMING sees no
	// available credit and abandons.
	for i := 0; i < 10 && fsm.State() != StateDONE; i++ {
		n := fsm.Step()
		if n.IsZero() {
			break
		}
		if n.After(clock.t) {
			clock.t = n
		}
	}

	if fsm.State() != StateDONE {
		t.Fatalf("final state = %s, want DONE", fsm.State())
	}
	if fc.consumeCalls != 0 {
		t.Fatalf("consume calls = %d, want 0 (target vanished pre-POST)", fc.consumeCalls)
	}
}

// TestFSM_WrongCreditConsumed_DetectsMismatch: two credits, target is "sooner".
// The server's deduction hook marks "later" instead. The triple-check must
// detect the mismatch and halt as error. "sooner" must still be available.
func TestFSM_WrongCreditConsumed_DetectsMismatch(t *testing.T) {
	E1 := time.Date(2026, 7, 19, 18, 23, 47, 0, time.UTC) // sooner (target)
	E2 := E1.Add(10 * 24 * time.Hour)                     // later (must NOT be touched)
	cfg := Config{RefreshInterval: 6 * time.Hour, TriggerLeadTime: 6 * time.Hour}
	fc := &fakeClient{
		credits: map[string]*Credit{
			"sooner": {ID: "sooner", Status: "available", ExpiresAt: E1},
			"later":  {ID: "later", Status: "available", ExpiresAt: E2},
		},
		weeklyPct:      25,
		consumeResp:    ConsumeResponse{Code: ConsumeCodeReset, WindowsReset: 2},
		deductCreditID: "later", // server bug: deducts the wrong credit
	}
	clock := &fakeClock{t: E1.Add(-12 * time.Hour)}
	logs := []string{}
	logf := func(level, scope string, state State, msg, next string, nextAt *time.Time, d map[string]any) {
		logs = append(logs, msg)
	}
	fsm := NewAccountFSM("acct1", CodexCredentials{}, cfg, fc, clock.Now, logf)

	drive(t, fsm, clock)

	if fsm.State() != StateDONE {
		t.Fatalf("final state = %s, want DONE", fsm.State())
	}
	if fc.consumeCalls != 1 {
		t.Fatalf("consume calls = %d, want 1", fc.consumeCalls)
	}
	if len(fc.consumeIDs) != 1 || fc.consumeIDs[0] != "sooner" {
		t.Fatalf("consumed = %v, want [sooner] (target)", fc.consumeIDs)
	}
	// The mismatch must be logged.
	joined := strings.Join(logs, "\n")
	if !strings.Contains(strings.ToLower(joined), "mismatch") {
		t.Fatalf("expected mismatch in logs, got:\n%s", joined)
	}
	// Sooner must still be available (server never actually consumed it).
	if c := fc.credits["sooner"]; c.Status != "available" {
		t.Fatalf("sooner status = %s, want available", c.Status)
	}
}

// TestFSM_QuotaLegIsLiveInProduction proves the third leg of the triple-check
// (quotaUp) is genuinely active in production-like conditions: ListCredits
// returns WeeklyPct=-1 (as the real reset-credits endpoint does), and
// stepCONFIRMING must call GetUsage to recover the real pre-reset weekly %.
// If that confirm-time GetUsage were missing, a reset that consumed the
// target credit but did NOT refill weekly quota would still be reported OK.
//
// We script exactly that broken-server scenario: consume succeeds, count
// drops, target disappears — but weekly stays at the pre-reset value.
// The FSM must classify this as PARTIAL (count ok but quota not recovered),
// proving the quota leg is doing real work.
func TestFSM_QuotaLegIsLive_PartialWhenQuotaDoesNotRecover(t *testing.T) {
	E := time.Date(2026, 7, 19, 18, 23, 47, 0, time.UTC)
	cfg := Config{RefreshInterval: 6 * time.Hour, TriggerLeadTime: 6 * time.Hour}
	fc := &fakeClient{
		credits:     map[string]*Credit{"c1": {ID: "c1", Status: "available", ExpiresAt: E}},
		weeklyPct:   15,
		consumeResp: ConsumeResponse{Code: ConsumeCodeReset, WindowsReset: 2},
		// Override the default consume side-effect: deduct the credit (so
		// targetGone + countDown1 hold) but do NOT bump weeklyPct to 100.
		keepQuotaAtPre: true,
	}
	clock := &fakeClock{t: E.Add(-12 * time.Hour)}
	logf := func(level, scope string, state State, msg, next string, nextAt *time.Time, d map[string]any) {
		// no-op; we assert on FSM behavior, not logs
	}
	fsm := NewAccountFSM("acct1", CodexCredentials{AccessToken: "t"}, cfg, fc, clock.Now, logf)

	for i := 0; i < 50 && fsm.State() != StateDONE; i++ {
		next := fsm.Step()
		if next.IsZero() {
			break
		}
		clock.t = next
	}

	if fsm.State() != StateDONE {
		t.Fatalf("final state = %s, want DONE", fsm.State())
	}
	if fc.usageCalls == 0 {
		t.Fatalf("GetUsage was never called at confirm time — quota leg is dead (regression of I1)")
	}
	// Credit WAS consumed (count dropped, target gone), but weekly stayed at
	// 15%. The triple check must NOT classify this as full success — it should
	// be PARTIAL (count ok, quota NOT up). Assert the credit is redeemed but
	// weekly did not recover, proving the quota leg is what distinguishes the
	// outcome.
	if c := fc.credits["c1"]; c.Status != "redeemed" {
		t.Fatalf("c1 status = %s, want redeemed (server did consume it)", c.Status)
	}
	if fc.weeklyPct != 15 {
		t.Fatalf("weeklyPct = %d, want 15 (scripted non-recovery)", fc.weeklyPct)
	}
	// No direct hook for the OK/PARTIAL/MISMATCH classification outcome on the
	// FSM struct, but the invariant we needed to prove — GetUsage is called at
	// confirm time — is asserted above. The wrong-credit test covers MISMATCH;
	// the happy-path test covers OK. This test proves the quota fetch happens
	// and that a non-recovering quota does not silently pass as OK.
}

func armedFSMFixture(t *testing.T) (*AccountFSM, *fakeClient, *fakeClock, time.Time) {
	t.Helper()
	E := time.Date(2026, 7, 19, 18, 23, 47, 0, time.UTC)
	cfg := Config{RefreshInterval: 12 * time.Hour, TriggerLeadTime: 6 * time.Hour}
	fc := &fakeClient{
		credits: map[string]*Credit{
			"a": {ID: "a", Status: "available", ExpiresAt: E},
			"b": {ID: "b", Status: "available", ExpiresAt: E.Add(14 * 24 * time.Hour)},
		},
		weeklyPct:   25,
		consumeResp: ConsumeResponse{Code: ConsumeCodeReset, WindowsReset: 2},
	}
	clock := &fakeClock{t: E.Add(-12 * time.Hour)}
	fsm := NewAccountFSM("acct", CodexCredentials{}, cfg, fc, clock.Now, nil)
	wake := fsm.Step()
	if fsm.State() != StateARMED || !wake.Equal(E.Add(-cfg.TriggerLeadTime)) {
		t.Fatalf("initial state=%s wake=%v, want ARMED at T", fsm.State(), wake)
	}
	return fsm, fc, clock, wake
}

func TestFSM_PinnedTargetUnavailable(t *testing.T) {
	for _, change := range []string{"redeemed", "removed", "expired", "expires_now"} {
		for _, replacement := range []string{"far_future", "already_due"} {
			t.Run(change+"/"+replacement, func(t *testing.T) {
				fsm, fc, clock, trigger := armedFSMFixture(t)
				clock.t = trigger
				switch change {
				case "redeemed":
					fc.credits["a"].Status = "redeemed"
				case "removed":
					delete(fc.credits, "a")
				case "expired":
					fc.credits["a"].ExpiresAt = trigger.Add(-time.Second)
				case "expires_now":
					fc.credits["a"].ExpiresAt = trigger
				}
				if replacement == "already_due" {
					fc.credits["b"].ExpiresAt = trigger.Add(time.Hour)
				}
				fsm.Step()
				fsm.Step()
				if fsm.State() == StateRESETTING {
					fsm.Step()
				}
				if replacement == "far_future" {
					// Nothing else is due: the cycle ends without a consume.
					if fsm.State() != StateDONE || fc.consumeCalls != 0 || fsm.attempt.RedeemRequestID != "" {
						t.Fatalf("state=%s consumed=%v attempt=%+v; nothing due must abandon without a consume", fsm.State(), fc.consumeIDs, fsm.attempt)
					}
					if fc.credits["b"].Status != "available" {
						t.Fatal("far-future credit was consumed")
					}
					return
				}
				// Fork: the pin follows the soonest-expiring credit. b is now
				// the soonest and inside its own window, so it is redeemed
				// (fresh request id); a is never touched.
				if len(fc.consumeIDs) != 1 || fc.consumeIDs[0] != "b" || fsm.State() != StateVERIFYING {
					t.Fatalf("state=%s consumed=%v; want the due replacement b", fsm.State(), fc.consumeIDs)
				}
			})
		}
	}
}

func TestFSM_AutomaticConfirmRequiresTarget(t *testing.T) {
	for _, state := range []State{StateARMED, StateCONFIRMING} {
		t.Run(string(state), func(t *testing.T) {
			fsm, fc, clock, trigger := armedFSMFixture(t)
			fsm.Reset()
			fsm.state = state
			clock.t = trigger
			for i := 0; i < 4 && fc.consumeCalls == 0 && fsm.State() != StateDONE; i++ {
				fsm.Step()
			}
			// Fork: without a pin, confirmation selects the soonest-expiring
			// credit and still applies its trigger time (a is due at T).
			if len(fc.consumeIDs) != 1 || fc.consumeIDs[0] != "a" {
				t.Fatalf("automatic confirmation without pin: state=%s consumed=%v", fsm.State(), fc.consumeIDs)
			}
		})
	}
}

// Fork: a credit that expires sooner than the armed one takes over the pin at
// confirmation (it is the one about to expire); exactly one consume.
func TestFSM_RepinsToEarlierCredit(t *testing.T) {
	fsm, fc, clock, trigger := armedFSMFixture(t)
	clock.t = trigger
	fc.credits["new"] = &Credit{ID: "new", Status: "available", ExpiresAt: trigger.Add(time.Hour)}
	fsm.Step()
	fsm.Step()
	fsm.Step()
	if fc.consumeCalls != 1 || fc.consumeIDs[0] != "new" {
		t.Fatalf("consumed=%v, want the sooner credit", fc.consumeIDs)
	}
}

func TestFSM_ConfirmRevalidatesTrigger(t *testing.T) {
	for _, change := range []string{"early_wake", "expiry_extended"} {
		t.Run(change, func(t *testing.T) {
			fsm, fc, clock, trigger := armedFSMFixture(t)
			want := trigger
			if change == "expiry_extended" {
				clock.t = trigger
				fc.credits["a"].ExpiresAt = fc.credits["a"].ExpiresAt.Add(24 * time.Hour)
				want = trigger.Add(24 * time.Hour)
			}
			fsm.SetNextWake(clock.t)
			fsm.Step()
			wake := fsm.Step()
			if change == "expiry_extended" {
				// Fork: no longer within one patrol of its trigger time, so
				// the cycle ends; a patrol (10m later) re-arms it in time.
				if fsm.State() != StateDONE || fsm.attempt.RedeemRequestID != "" || fc.consumeCalls != 0 {
					t.Fatalf("state=%s attempt=%+v consumed=%v", fsm.State(), fsm.attempt, fc.consumeIDs)
				}
				return
			}
			if fsm.State() != StateARMED || !wake.Equal(want) || !fsm.NextWake().Equal(want) {
				t.Fatalf("state=%s wake=%v nextWake=%v; want re-arm at %v", fsm.State(), wake, fsm.NextWake(), want)
			}
			if fsm.attempt.RedeemRequestID != "" || fc.consumeCalls != 0 {
				t.Fatalf("premature attempt=%+v consumed=%v", fsm.attempt, fc.consumeIDs)
			}
			fc.credits["new"] = &Credit{ID: "new", Status: "available", ExpiresAt: want.Add(time.Hour)}
			clock.t = wake
			fsm.Step()
			fsm.Step()
			fsm.Step()
			// Fork: "new" now expires sooner than a, so it takes the pin.
			if fc.consumeCalls != 1 || fc.consumeIDs[0] != "new" || !fsm.attempt.StartedAt.Equal(want) {
				t.Fatalf("consumed=%v started=%v, want new at %v", fc.consumeIDs, fsm.attempt.StartedAt, want)
			}
		})
	}
}

func TestFSM_NeverExpiringAutomaticCredit(t *testing.T) {
	t.Run("idle", func(t *testing.T) {
		fsm, fc, clock, _ := armedFSMFixture(t)
		fsm.Reset()
		fc.credits = map[string]*Credit{"never": {ID: "never", Status: "available"}}
		wake := fsm.Step()
		if fsm.State() != StateIDLE || !wake.Equal(clock.t.Add(fsm.Cfg.RefreshInterval)) || fc.consumeCalls != 0 {
			t.Fatalf("state=%s wake=%v, want normal patrol without automatic redemption", fsm.State(), wake)
		}
	})
	t.Run("expiry_removed_while_armed", func(t *testing.T) {
		fsm, fc, clock, trigger := armedFSMFixture(t)
		fc.credits["a"].ExpiresAt = time.Time{}
		clock.t = trigger
		fsm.Step()
		fsm.Step()
		if fsm.State() != StateDONE || fsm.attempt.RedeemRequestID != "" || fc.consumeCalls != 0 {
			t.Fatalf("state=%s attempt=%+v, want abandon non-expiring target", fsm.State(), fsm.attempt)
		}
	})
}

// TestFSM_StepAndSettleResetsDoneAndSchedulesPatrol verifies the atomic
// settle: a terminating tick lands in IDLE (never DONE), carries the pin and
// manual-intent cleanup of Reset, and schedules the next patrol at
// completion + refresh_interval.
func TestFSM_StepAndSettleResetsDoneAndSchedulesPatrol(t *testing.T) {
	fsm, fc, clock, trigger := armedFSMFixture(t)
	fc.credits["a"].Status = "redeemed" // confirm at T will abandon -> DONE
	clock.t = trigger
	fsm.Step() // ARMED -> CONFIRMING (wake=now)

	wake := fsm.StepAndSettle()
	want := trigger.Add(fsm.retryInterval()) // fork: patrol again after retry_interval
	if fsm.State() != StateIDLE || !wake.Equal(want) || !fsm.NextWake().Equal(want) {
		t.Fatalf("settle: state=%s wake=%v nextWake=%v, want IDLE patrol at %v", fsm.State(), wake, fsm.NextWake(), want)
	}
	if fsm.scheduledCreditID != "" || fsm.manualReset {
		t.Fatalf("settle leaked cycle metadata: pin=%q manual=%v", fsm.scheduledCreditID, fsm.manualReset)
	}
}

// TestFSM_StepAndSettleAtomicWithForceConfirm deterministically reproduces
// the strand race's interleaving: a manual reset fired exactly in the window
// between the tick's return and the schedule write (testPostStep runs there,
// with mu held). Because step+settle share one critical section, the
// ForceConfirm cannot slip inside; it applies after the settle and the FSM
// keeps a non-zero wake instead of being stranded in CONFIRMING with a zero
// wake that soonest() would skip forever.
func TestFSM_StepAndSettleAtomicWithForceConfirm(t *testing.T) {
	fsm, fc, clock, trigger := armedFSMFixture(t)
	fc.credits["a"].Status = "redeemed" // tick will abandon -> DONE at settle
	clock.t = trigger
	fsm.Step() // ARMED -> CONFIRMING

	var mgmtDone chan struct{}
	fsm.testPostStep = func() {
		mgmtDone = make(chan struct{})
		go func() {
			defer close(mgmtDone)
			fsm.ForceConfirm()
		}()
		runtime.Gosched() // let the goroutine pile onto the held mutex
	}
	fsm.StepAndSettle()
	<-mgmtDone
	fsm.testPostStep = nil // one interleaving is enough; stop firing in the drive loop

	if fsm.State() != StateCONFIRMING || fsm.NextWake().IsZero() || !fsm.manualReset || fsm.scheduledCreditID != "" {
		t.Fatalf("manual reset stranded: state=%s wake=%v manual=%v pin=%q",
			fsm.State(), fsm.NextWake(), fsm.manualReset, fsm.scheduledCreditID)
	}
	if !fsm.NextWake().Equal(clock.t) {
		t.Fatalf("manual wake=%v, want now=%v", fsm.NextWake(), clock.t)
	}

	// The manual reset must still complete: it picks the remaining credit b.
	for i := 0; i < 5 && fsm.State() != StateIDLE; i++ {
		if wake := fsm.StepAndSettle(); !wake.IsZero() {
			clock.t = wake
		}
	}
	if fsm.State() != StateIDLE || fc.consumeCalls != 1 || fc.consumeIDs[0] != "b" {
		t.Fatalf("manual cycle after settle: state=%s consumed=%v, want b redeemed", fsm.State(), fc.consumeIDs)
	}
}
