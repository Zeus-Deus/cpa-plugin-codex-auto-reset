package main

// Tests for the fork's behaviour: 1h lead, 10m retries bounded by the pinned
// credit's expiry, nothing_to_reset retried with a fresh idempotency key, and
// the paused switch.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func forkCfg() Config {
	return Config{RefreshInterval: 6 * time.Hour, TriggerLeadTime: time.Hour, RetryInterval: 10 * time.Minute}
}

// Only the soonest-expiring credit is ever sent, never before expiry-1h and
// never at/after expiry; a far-future credit and a non-expiring one are never
// touched.
func TestFork_OnlyPinnedCreditInsideWindow(t *testing.T) {
	E := time.Date(2026, 11, 20, 15, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: E.Add(-30 * time.Hour)}
	fc := &fakeClient{
		credits: map[string]*Credit{
			"soon":  {ID: "soon", Status: "available", ExpiresAt: E},
			"later": {ID: "later", Status: "available", ExpiresAt: E.Add(20 * 24 * time.Hour)},
			"never": {ID: "never", Status: "available"},
		},
		weeklyPct:   30,
		consumeResp: ConsumeResponse{Code: ConsumeCodeReset, WindowsReset: 2},
		clock:       clock,
	}
	fsm := NewAccountFSM("acct", CodexCredentials{}, forkCfg(), fc, clock.Now, nil)
	// Drive with patrol settles, like the worker does, until the cycle ends.
	for i := 0; i < 50 && fc.consumeCalls == 0; i++ {
		next := fsm.StepAndSettle()
		if next.After(clock.t) {
			clock.t = next
		}
	}
	for i := 0; i < 10 && fsm.State() != StateIDLE; i++ {
		if next := fsm.StepAndSettle(); next.After(clock.t) {
			clock.t = next
		}
	}
	if fc.consumeCalls != 1 || fc.consumeIDs[0] != "soon" {
		t.Fatalf("consumed %v, want exactly [soon]", fc.consumeIDs)
	}
	at := fc.consumeTimes[0]
	if at.Before(E.Add(-time.Hour)) || !at.Before(E) {
		t.Fatalf("consumed at %v, want within [E-1h, E) of %v", at, E)
	}
	if fc.credits["later"].Status != "available" || fc.credits["never"].Status != "available" {
		t.Fatal("a non-target credit changed")
	}
}

// nothing_to_reset in an automatic cycle: the credit is kept, the same pinned
// credit is retried every 10m with a NEW redeem_request_id, and the cycle
// stops before expiry.
func TestFork_NothingToResetRetriesUntilExpiry(t *testing.T) {
	E := time.Date(2026, 11, 20, 15, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: E.Add(-2 * time.Hour)}
	fc := &fakeClient{
		credits:     map[string]*Credit{"c1": {ID: "c1", Status: "available", ExpiresAt: E}},
		weeklyPct:   95,
		consumeResp: ConsumeResponse{Code: ConsumeCodeNothingToReset},
		clock:       clock,
	}
	fsm := NewAccountFSM("acct", CodexCredentials{}, forkCfg(), fc, clock.Now, nil)
	drive(t, fsm, clock)
	if fsm.State() != StateDONE {
		t.Fatalf("state = %s, want DONE", fsm.State())
	}
	if fc.consumeCalls != 6 {
		t.Fatalf("consume calls = %d, want 6 (E-1h .. E-10m)", fc.consumeCalls)
	}
	seen := map[string]bool{}
	for i, rid := range fc.redeemIDs {
		if seen[rid] {
			t.Fatalf("redeem id reused after nothing_to_reset: %v", fc.redeemIDs)
		}
		seen[rid] = true
		if fc.consumeIDs[i] != "c1" {
			t.Fatalf("consume %d targeted %q", i, fc.consumeIDs[i])
		}
		if got, want := fc.consumeTimes[i], E.Add(-time.Hour).Add(time.Duration(i)*10*time.Minute); !got.Equal(want) {
			t.Fatalf("consume %d at %v, want %v", i, got, want)
		}
	}
	if fc.credits["c1"].Status != "available" {
		t.Fatal("credit must not be consumed by nothing_to_reset")
	}
}

// Heavy use during the window: nothing_to_reset twice, then the third try
// redeems and verifies.
func TestFork_NothingToResetThenReset(t *testing.T) {
	E := time.Date(2026, 11, 20, 15, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: E.Add(-2 * time.Hour)}
	fc := &fakeClient{
		credits:   map[string]*Credit{"c1": {ID: "c1", Status: "available", ExpiresAt: E}},
		weeklyPct: 60,
		consumeScript: []ConsumeResponse{
			{Code: ConsumeCodeNothingToReset}, {Code: ConsumeCodeNothingToReset},
		},
		consumeResp: ConsumeResponse{Code: ConsumeCodeReset, WindowsReset: 2},
		clock:       clock,
	}
	var logs []string
	logf := func(level, scope string, state State, msg, nextAction string, nextAt *time.Time, details map[string]any) {
		logs = append(logs, msg)
	}
	fsm := NewAccountFSM("acct", CodexCredentials{}, forkCfg(), fc, clock.Now, logf)
	drive(t, fsm, clock)
	if fc.consumeCalls != 3 || fc.credits["c1"].Status != "redeemed" {
		t.Fatalf("calls=%d status=%s logs=%v", fc.consumeCalls, fc.credits["c1"].Status, logs)
	}
	rec := fsm.TakePendingHistory()
	if rec == nil || !rec.Success {
		t.Fatalf("history = %+v, want success; logs=%v", rec, logs)
	}
}

// Transient errors reuse the same redeem_request_id and stop before expiry.
func TestFork_TransientRetrySameKeyBoundedByExpiry(t *testing.T) {
	E := time.Date(2026, 11, 20, 15, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: E.Add(-40 * time.Minute)} // late start inside window
	fc := &fakeClient{
		credits:    map[string]*Credit{"c1": {ID: "c1", Status: "available", ExpiresAt: E}},
		weeklyPct:  10,
		failFirstN: 100,
		failErr:    &HTTPStatusError{Code: 503},
		clock:      clock,
	}
	fsm := NewAccountFSM("acct", CodexCredentials{}, forkCfg(), fc, clock.Now, nil)
	drive(t, fsm, clock)
	if fc.consumeCalls != 4 { // E-40m, -30m, -20m, -10m
		t.Fatalf("consume calls = %d, want 4", fc.consumeCalls)
	}
	for _, rid := range fc.redeemIDs {
		if rid != fc.redeemIDs[0] {
			t.Fatalf("transient retries must reuse the key: %v", fc.redeemIDs)
		}
	}
	for _, at := range fc.consumeTimes {
		if !at.Before(E) {
			t.Fatalf("consume at %v, not before expiry %v", at, E)
		}
	}
}

// A permanent 4xx stops immediately.
func TestFork_PermanentClientErrorStops(t *testing.T) {
	E := time.Date(2026, 11, 20, 15, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: E.Add(-2 * time.Hour)}
	fc := &fakeClient{
		credits:    map[string]*Credit{"c1": {ID: "c1", Status: "available", ExpiresAt: E}},
		failFirstN: 100,
		failErr:    &HTTPStatusError{Code: 400},
	}
	fsm := NewAccountFSM("acct", CodexCredentials{}, forkCfg(), fc, clock.Now, nil)
	drive(t, fsm, clock)
	if fc.consumeCalls != 1 || fsm.State() != StateDONE {
		t.Fatalf("calls=%d state=%s", fc.consumeCalls, fsm.State())
	}
}

// A manual reset that gets nothing_to_reset stops (no loop).
func TestFork_ManualNothingToResetStops(t *testing.T) {
	E := time.Date(2026, 11, 20, 15, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: E.Add(-5 * 24 * time.Hour)}
	fc := &fakeClient{
		credits:     map[string]*Credit{"c1": {ID: "c1", Status: "available", ExpiresAt: E}},
		weeklyPct:   99,
		consumeResp: ConsumeResponse{Code: ConsumeCodeNothingToReset},
	}
	fsm := NewAccountFSM("acct", CodexCredentials{}, forkCfg(), fc, clock.Now, nil)
	fsm.ForceConfirm()
	drive(t, fsm, clock)
	if fc.consumeCalls != 1 || fsm.State() != StateDONE {
		t.Fatalf("calls=%d state=%s", fc.consumeCalls, fsm.State())
	}
}

// Paused: a credit inside its window is reported but never armed or sent;
// resuming through the worker redeems it right away. Manual reset still works
// while paused.
func TestFork_PausedNeverConsumesAutomatically(t *testing.T) {
	E := time.Date(2026, 11, 20, 15, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: E.Add(-30 * time.Minute)}
	fc := &fakeClient{
		credits:     map[string]*Credit{"c1": {ID: "c1", Status: "available", ExpiresAt: E}},
		weeklyPct:   20,
		consumeResp: ConsumeResponse{Code: ConsumeCodeReset, WindowsReset: 2},
	}
	cfg := forkCfg()
	cfg.Paused = true
	w := NewWorker(cfg, []string{"acct"}, func(string) (CodexCredentials, ResetClient, error) {
		return CodexCredentials{}, fc, nil
	}, clock.Now)
	w.ensureFSM("acct")
	for i := 0; i < 5; i++ {
		w.stepAccountSafe("acct")
	}
	fsm := w.fsms["acct"]
	if fc.consumeCalls != 0 || fsm.State() != StateIDLE {
		t.Fatalf("paused: calls=%d state=%s", fc.consumeCalls, fsm.State())
	}
	w.SetPaused(false)
	if !fsm.NextWake().Equal(clock.t) {
		t.Fatalf("resume did not pull an immediate patrol: wake=%v", fsm.NextWake())
	}
	for i := 0; i < 6 && fc.consumeCalls == 0; i++ {
		w.stepAccountSafe("acct")
	}
	if fc.consumeCalls != 1 || fc.consumeIDs[0] != "c1" {
		t.Fatalf("after resume: calls=%v", fc.consumeIDs)
	}
}

// Pausing while ARMED cancels the cycle at the next step with no consume.
func TestFork_PauseWhileArmedCancels(t *testing.T) {
	w, fsm, fc, clock, trigger := armedWorkerFixture(t)
	fsm.PausedFn = w.paused.Load
	w.SetPaused(true)
	clock.t = trigger
	for i := 0; i < 4; i++ {
		w.stepAccountSafe("acct")
	}
	if fc.consumeCalls != 0 || fsm.State() != StateIDLE {
		t.Fatalf("calls=%d state=%s", fc.consumeCalls, fsm.State())
	}
}

func TestFork_ManualResetWorksWhilePaused(t *testing.T) {
	E := time.Date(2026, 11, 20, 15, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: E.Add(-3 * 24 * time.Hour)}
	fc := &fakeClient{
		credits:     map[string]*Credit{"c1": {ID: "c1", Status: "available", ExpiresAt: E}},
		weeklyPct:   5,
		consumeResp: ConsumeResponse{Code: ConsumeCodeReset, WindowsReset: 2},
	}
	cfg := forkCfg()
	cfg.Paused = true
	fsm := NewAccountFSM("acct", CodexCredentials{}, cfg, fc, clock.Now, nil)
	fsm.ForceConfirm()
	drive(t, fsm, clock)
	if fc.consumeCalls != 1 || fc.credits["c1"].Status != "redeemed" {
		t.Fatalf("manual while paused: calls=%d", fc.consumeCalls)
	}
}

// PUT /settings is a partial update (does not wipe enabled_accounts or the
// paused switch); PUT /paused persists.
func TestFork_SettingsPartialAndPausedRoute(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	cfg := DefaultConfig()
	cfg.EnabledAccounts = []string{"a1", "a2"}
	st := &PluginState{Config: cfg, Accounts: map[string]AccountRuntime{}}
	h := newManagementHandlers(st, path, nil)

	code, body, _ := h.handle("PUT", "/v0/management/plugins/codex-auto-reset/paused", nil, []byte(`{"paused":true}`))
	if code != 200 || !st.Config.Paused {
		t.Fatalf("pause: %d %s", code, body)
	}
	code, body, _ = h.handle("PUT", "/v0/management/plugins/codex-auto-reset/settings", nil, []byte(`{"retry_interval":"15m"}`))
	if code != 200 {
		t.Fatalf("settings: %d %s", code, body)
	}
	if st.Config.RetryInterval != 15*time.Minute || len(st.Config.EnabledAccounts) != 2 || !st.Config.Paused || st.Config.TriggerLeadTime != time.Hour {
		t.Fatalf("partial PUT clobbered config: %+v", st.Config)
	}
	code, _, _ = h.handle("PUT", "/v0/management/plugins/codex-auto-reset/settings", nil, []byte(`{"trigger_lead_time":"30s"}`))
	if code != 400 {
		t.Fatalf("sub-minute lead accepted: %d", code)
	}
	code, _, _ = h.handle("PUT", "/v0/management/plugins/codex-auto-reset/paused", nil, []byte(`{}`))
	if code != 400 {
		t.Fatalf("empty paused body accepted: %d", code)
	}
	loaded, _, err := loadState(path)
	if err != nil || !loaded.Config.Paused || loaded.Config.RetryInterval != 15*time.Minute {
		t.Fatalf("persisted = %+v err=%v", loaded.Config, err)
	}
	raw, _ := json.Marshal(loaded.Config)
	var round Config
	if err := json.Unmarshal(raw, &round); err != nil || !round.Paused || round.RetryInterval != 15*time.Minute {
		t.Fatalf("round trip = %+v err=%v (%s)", round, err, raw)
	}
	for _, route := range []string{"debug/usage", "debug/auth-list"} {
		if code, _, _ := h.handle("GET", "/v0/management/plugins/codex-auto-reset/"+route, nil, nil); code != 404 {
			t.Fatalf("%s still routed: %d", route, code)
		}
	}
}

func TestFork_UserAgentOverride(t *testing.T) {
	defer setUserAgent("")
	if userAgent() != codexUserAgent {
		t.Fatalf("default UA = %q", userAgent())
	}
	setUserAgent(" codex_cli_rs/0.165.0 (Arch Linux; x86_64) ")
	if userAgent() != "codex_cli_rs/0.165.0 (Arch Linux; x86_64)" {
		t.Fatalf("override UA = %q", userAgent())
	}
}

// lostResponseClient: the first consume lands server-side but the caller sees
// a network error (lost response).
type lostResponseClient struct{ *fakeClient }

func (l lostResponseClient) Consume(c CodexCredentials, rrid, cid string) (ConsumeResponse, error) {
	l.consumeCalls++
	l.consumeIDs = append(l.consumeIDs, cid)
	l.redeemIDs = append(l.redeemIDs, rrid)
	if l.consumeCalls == 1 {
		l.credits[cid].Status = "redeemed"
		l.weeklyPct = 100
		return ConsumeResponse{}, errString("read: connection reset by peer")
	}
	return ConsumeResponse{Code: ConsumeCodeAlreadyRedeemed}, nil
}

// After an ambiguous attempt, the retry re-lists first; the pinned credit is
// gone, so it verifies instead of sending a second consume.
func TestFork_RetryRelistsAndVerifiesInsteadOfResending(t *testing.T) {
	E := time.Date(2026, 11, 20, 15, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: E.Add(-2 * time.Hour)}
	base := &fakeClient{
		credits:   map[string]*Credit{"c1": {ID: "c1", Status: "available", ExpiresAt: E}, "c2": {ID: "c2", Status: "available", ExpiresAt: E.Add(48 * time.Hour)}},
		weeklyPct: 40,
	}
	fsm := NewAccountFSM("acct", CodexCredentials{}, forkCfg(), lostResponseClient{base}, clock.Now, nil)
	drive(t, fsm, clock)
	if base.consumeCalls != 1 {
		t.Fatalf("consume calls = %d, want 1 (no resend after the credit vanished)", base.consumeCalls)
	}
	rec := fsm.TakePendingHistory()
	if rec == nil || !rec.Success || rec.CreditID != "c1" {
		t.Fatalf("history = %+v", rec)
	}
	if base.credits["c2"].Status != "available" {
		t.Fatal("second credit touched")
	}
}

// A stopped (replaced) worker's FSM never sends a consume.
func TestFork_HaltedFSMSendsNoConsume(t *testing.T) {
	w, fsm, fc, clock, trigger := armedWorkerFixture(t)
	fsm.Halted = w.isStopped
	clock.t = trigger
	w.stepAccountSafe("acct") // ARMED -> CONFIRMING
	w.stepAccountSafe("acct") // CONFIRMING -> RESETTING
	w.Stop()
	w.stepAccountSafe("acct")
	if fc.consumeCalls != 0 {
		t.Fatalf("stopped worker sent %d consume(s)", fc.consumeCalls)
	}
}

func TestFork_StopAndWaitJoinsRun(t *testing.T) {
	w := NewWorker(forkCfg(), nil, func(string) (CodexCredentials, ResetClient, error) {
		return CodexCredentials{}, &fakeClient{credits: map[string]*Credit{}}, nil
	}, time.Now)
	go w.Run()
	time.Sleep(20 * time.Millisecond)
	start := time.Now()
	w.StopAndWait(5 * time.Second)
	select {
	case <-w.doneCh:
	default:
		t.Fatal("Run did not return")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("StopAndWait took too long")
	}
	// Never-started worker: returns immediately.
	w2 := NewWorker(forkCfg(), nil, nil, time.Now)
	w2.StopAndWait(5 * time.Second)
}

// Unknown post-reset quota is not "recovered" (no cooldown clearing on a
// guess).
func TestFork_UnknownQuotaIsNotRecovered(t *testing.T) {
	snap, err := parseUsageResponse([]byte(`{"rate_limit":null}`), time.Now())
	if err != nil || snap.WeeklyPct != -1 {
		t.Fatalf("snap=%+v err=%v", snap, err)
	}
}

// An unparseable expiry is never targeted (not treated as "never expires").
func TestFork_InvalidExpiryNeverTargeted(t *testing.T) {
	now := time.Date(2026, 11, 20, 15, 0, 0, 0, time.UTC)
	snap, err := parseResetCreditsResponse([]byte(`{"credits":[{"id":"bad","status":"available","expires_at":"next tuesday"},{"id":"ok","status":"available","expires_at":"2026-11-21T15:00:00.123Z"}],"available_count":2}`), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Credits) != 1 || snap.Credits[0].ID != "ok" {
		t.Fatalf("credits = %+v", snap.Credits)
	}
}

func TestFork_LeadAndRetryClamped(t *testing.T) {
	cfg := normalizeConfig(Config{RefreshInterval: time.Hour, TriggerLeadTime: 720 * time.Hour, RetryInterval: 5 * time.Hour})
	if cfg.TriggerLeadTime != 2*time.Hour || cfg.RetryInterval != 30*time.Minute {
		t.Fatalf("cfg = %+v", cfg)
	}
	h := newManagementHandlers(&PluginState{Config: DefaultConfig(), Accounts: map[string]AccountRuntime{}}, filepath.Join(t.TempDir(), "s.json"), nil)
	if code, _, _ := h.handle("PUT", "/v0/management/plugins/codex-auto-reset/settings", nil, []byte(`{"trigger_lead_time":"3h"}`)); code != 400 {
		t.Fatalf("3h lead accepted: %d", code)
	}
}

// Redirects are not followed (the token must not leave the documented host).
func TestFork_RedirectsRefused(t *testing.T) {
	hits := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ }))
	defer target.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/steal", http.StatusTemporaryRedirect)
	}))
	defer srv.Close()
	c := &OpenAIClient{BaseURL: srv.URL}
	_, err := c.Consume(CodexCredentials{AccessToken: "tok"}, "rid", "cid")
	if err == nil || hits != 0 {
		t.Fatalf("err=%v hits=%d", err, hits)
	}
}

// Every handler route is registered with the host (CPA only forwards
// registered routes) and every registered route has a handler.
func TestFork_RegisteredRoutesMatchHandlers(t *testing.T) {
	h := newManagementHandlers(&PluginState{Config: DefaultConfig(), Accounts: map[string]AccountRuntime{}}, filepath.Join(t.TempDir(), "s.json"), nil)
	reg := managementRegistrationResponse()
	seen := map[string]bool{}
	for _, r := range reg.Routes {
		seen[r.Method+" "+r.Path] = true
		code, body, _ := h.handle(r.Method, "/v0/management"+r.Path, nil, []byte(`{}`))
		if code == 404 && string(body) == `{"error":"route not found"}` {
			t.Fatalf("registered route has no handler: %s %s", r.Method, r.Path)
		}
	}
	for _, want := range []string{"PUT /plugins/codex-auto-reset/paused", "PUT /plugins/codex-auto-reset/settings", "POST /plugins/codex-auto-reset/reset"} {
		if !seen[want] {
			t.Fatalf("route not registered: %s", want)
		}
	}
	for k := range seen {
		if strings.Contains(k, "/debug/") {
			t.Fatalf("debug route still registered: %s", k)
		}
	}
}

// A credit that expires sooner than the armed one is picked up at
// confirmation (re-pin), never left to expire.
func TestFork_RepinsToSoonerCredit(t *testing.T) {
	E := time.Date(2026, 11, 20, 15, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: E.Add(-3 * time.Hour)}
	fc := &fakeClient{
		credits:     map[string]*Credit{"a": {ID: "a", Status: "available", ExpiresAt: E}},
		weeklyPct:   30,
		consumeResp: ConsumeResponse{Code: ConsumeCodeReset, WindowsReset: 1},
		clock:       clock,
	}
	fsm := NewAccountFSM("acct", CodexCredentials{}, forkCfg(), fc, clock.Now, nil)
	fsm.Step() // IDLE -> ARMED on "a" (T = E-1h)
	if fsm.State() != StateARMED {
		t.Fatalf("state = %s", fsm.State())
	}
	// A sooner credit "b" appears (expires at E-30m).
	fc.credits["b"] = &Credit{ID: "b", Status: "available", ExpiresAt: E.Add(-30 * time.Minute)}
	drive(t, fsm, clock)
	if len(fc.consumeIDs) != 1 || fc.consumeIDs[0] != "b" {
		t.Fatalf("consumed %v, want [b]", fc.consumeIDs)
	}
	if at := fc.consumeTimes[0]; at.Before(E.Add(-90*time.Minute)) || !at.Before(E.Add(-30*time.Minute)) {
		t.Fatalf("b consumed at %v outside its own window", at)
	}
	if fc.credits["a"].Status != "available" {
		t.Fatal("a must be untouched in this cycle")
	}
}

// After a cycle ends, the next patrol is retry_interval later, so a second
// credit expiring soon after the first still gets its own cycle.
func TestFork_SecondCreditSoonAfterGetsItsCycle(t *testing.T) {
	E := time.Date(2026, 11, 20, 15, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: E.Add(-2 * time.Hour)}
	fc := &fakeClient{
		credits: map[string]*Credit{
			"a": {ID: "a", Status: "available", ExpiresAt: E},
			"b": {ID: "b", Status: "available", ExpiresAt: E.Add(2 * time.Hour)},
		},
		weeklyPct:   10,
		consumeResp: ConsumeResponse{Code: ConsumeCodeReset, WindowsReset: 1},
		clock:       clock,
	}
	fsm := NewAccountFSM("acct", CodexCredentials{}, forkCfg(), fc, clock.Now, nil)
	for i := 0; i < 100 && fc.consumeCalls < 2; i++ {
		if next := fsm.StepAndSettle(); next.After(clock.t) {
			clock.t = next
		}
		if fsm.State() == StateVERIFYING {
			fc.weeklyPct = 10 // used up again before b's window
		}
	}
	if len(fc.consumeIDs) != 2 || fc.consumeIDs[0] != "a" || fc.consumeIDs[1] != "b" {
		t.Fatalf("consumed %v, want [a b]", fc.consumeIDs)
	}
	if at := fc.consumeTimes[1]; at.Before(E.Add(time.Hour)) || !at.Before(E.Add(2*time.Hour)) {
		t.Fatalf("b consumed at %v outside its window", at)
	}
}

// Manual Reset during a pending automatic retry takes it over (same credit,
// same request id); during verification it answers 409.
func TestFork_ManualTakesOverPendingRetry(t *testing.T) {
	w, fsm, fc, clock, trigger := armedWorkerFixture(t)
	fc.failFirstN = 1
	fc.failErr = &HTTPStatusError{Code: 503}
	clock.t = trigger
	w.stepAccountSafe("acct") // ARMED -> CONFIRMING
	w.stepAccountSafe("acct") // -> RESETTING
	w.stepAccountSafe("acct") // POST fails -> retry in 10m
	if fsm.State() != StateRESETTING || fc.consumeCalls != 1 {
		t.Fatalf("state=%s calls=%d", fsm.State(), fc.consumeCalls)
	}
	if err := w.ForceConfirm("acct"); err != nil {
		t.Fatal(err)
	}
	if !fsm.NextWake().Equal(clock.t) {
		t.Fatalf("manual takeover not immediate: %v", fsm.NextWake())
	}
	w.stepAccountSafe("acct")
	if fc.consumeCalls != 2 || fc.redeemIDs[0] != fc.redeemIDs[1] || fc.consumeIDs[1] != fc.consumeIDs[0] {
		t.Fatalf("takeover must reuse target and key: ids=%v keys=%v", fc.consumeIDs, fc.redeemIDs)
	}
	if fsm.State() != StateVERIFYING {
		t.Fatalf("state=%s", fsm.State())
	}
	if err := w.ForceConfirm("acct"); err != errCycleBusy {
		t.Fatalf("ForceConfirm during verify = %v", err)
	}
}

func TestFork_ForcedPauseFromYAMLCannotBeResumed(t *testing.T) {
	forcedPaused.Store(true)
	defer forcedPaused.Store(false)
	h := newManagementHandlers(&PluginState{Config: DefaultConfig(), Accounts: map[string]AccountRuntime{}}, filepath.Join(t.TempDir(), "s.json"), nil)
	if code, _, _ := h.handle("PUT", "/v0/management/plugins/codex-auto-reset/paused", nil, []byte(`{"paused":false}`)); code != 409 {
		t.Fatalf("resume under forced pause = %d", code)
	}
}

func TestFork_MalformedCreditsRejected(t *testing.T) {
	for _, body := range []string{`{}`, `{"credits":[]}`, `{"available_count":1}`, `{"credits":[{"status":"available"}],"available_count":1}`} {
		if _, err := parseResetCreditsResponse([]byte(body), time.Now()); err == nil {
			t.Fatalf("accepted %s", body)
		}
	}
}

// An older snapshot never overwrites a newer one on disk.
func TestFork_StateWritesAreOrdered(t *testing.T) {
	t.Cleanup(func() { lastWrittenPath, lastWrittenSeq = "", 0 })
	path := filepath.Join(t.TempDir(), "state.json")
	if err := writeStateFileSeq(path, []byte(`{"v":2}`), 1<<62); err != nil {
		t.Fatal(err)
	}
	if err := writeStateFileSeq(path, []byte(`{"v":1}`), 1<<61); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)
	if string(raw) != `{"v":2}` {
		t.Fatalf("stale snapshot won: %s", raw)
	}
}

// The config_yaml hard pause gates automatic consumes even if the live switch
// says "on" (e.g. a resume raced a reload).
func TestFork_ForcedPauseGatesConsume(t *testing.T) {
	forcedPaused.Store(true)
	defer forcedPaused.Store(false)
	E := time.Date(2026, 11, 20, 15, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: E.Add(-30 * time.Minute)}
	fc := &fakeClient{
		credits:     map[string]*Credit{"c1": {ID: "c1", Status: "available", ExpiresAt: E}},
		weeklyPct:   20,
		consumeResp: ConsumeResponse{Code: ConsumeCodeReset},
	}
	w := NewWorker(forkCfg(), []string{"acct"}, func(string) (CodexCredentials, ResetClient, error) {
		return CodexCredentials{}, fc, nil
	}, clock.Now)
	w.ensureFSM("acct")
	for i := 0; i < 5; i++ {
		w.stepAccountSafe("acct")
	}
	if fc.consumeCalls != 0 {
		t.Fatalf("consumed under forced pause: %v", fc.consumeIDs)
	}
}
