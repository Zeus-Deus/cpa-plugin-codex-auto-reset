package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds the user-tunable fields. The scheduling fields and the paused
// switch are editable from the management UI; state_dir and user_agent are
// operator-only overrides that come from CPA's config_yaml on every load. All
// other behavior is hardcoded in constants.go (endpoints, post-reset delay, log
// limits). Endpoints and OpenAI parameters are NEVER user-configurable.
type Config struct {
	RefreshInterval time.Duration `yaml:"refresh_interval" json:"-"`
	TriggerLeadTime time.Duration `yaml:"trigger_lead_time" json:"-"`
	// RetryInterval is the wait between redeem attempts for the pinned credit
	// (transient errors, and nothing_to_reset in automatic cycles). Automatic
	// retries stop once the next attempt would land at or after the credit's
	// expiry.
	RetryInterval   time.Duration `yaml:"retry_interval" json:"-"`
	EnabledAccounts []string      `yaml:"enabled_accounts" json:"enabled_accounts"`
	// Paused stops automatic redemption (patrols keep running read-only, and a
	// manual Reset still works). Persisted, so it survives restarts.
	Paused    bool   `yaml:"paused" json:"paused"`
	StateDir  string `yaml:"state_dir" json:"state_dir,omitempty"`
	UserAgent string `yaml:"user_agent" json:"-"`
}

// MarshalJSON renders Duration fields as human-readable strings ("12h",
// "6h", "1h30m") instead of Go's default nanosecond integer, which is
// unreadable in the management UI's raw JSON view.
func (c Config) MarshalJSON() ([]byte, error) {
	type alias struct {
		RefreshInterval string   `json:"refresh_interval"`
		TriggerLeadTime string   `json:"trigger_lead_time"`
		RetryInterval   string   `json:"retry_interval"`
		EnabledAccounts []string `json:"enabled_accounts"`
		Paused          bool     `json:"paused"`
		StateDir        string   `json:"state_dir,omitempty"`
	}
	return json.Marshal(alias{
		RefreshInterval: formatDuration(c.RefreshInterval),
		TriggerLeadTime: formatDuration(c.TriggerLeadTime),
		RetryInterval:   formatDuration(c.RetryInterval),
		EnabledAccounts: c.EnabledAccounts,
		Paused:          c.Paused,
		StateDir:        c.StateDir,
	})
}

// formatDuration trims Go's verbose "12h0m0s" form to the minimal "12h".
// "1h30m0s" -> "1h30m", "90m0s" -> "90m", "45s" stays. Mirrors the scheduler
// plugin's formatDuration so duration displays are consistent across plugins.
func formatDuration(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = s[:len(s)-2]
	}
	if strings.HasSuffix(s, "h0m") {
		s = s[:len(s)-2]
	}
	return s
}

// UnmarshalJSON accepts both the string form ("12h") and the raw nanosecond
// integer form (43200000000000) for Duration fields, for round-trip safety.
func (c *Config) UnmarshalJSON(raw []byte) error {
	type alias struct {
		RefreshInterval json.RawMessage `json:"refresh_interval"`
		TriggerLeadTime json.RawMessage `json:"trigger_lead_time"`
		RetryInterval   json.RawMessage `json:"retry_interval"`
		EnabledAccounts []string        `json:"enabled_accounts"`
		Paused          bool            `json:"paused"`
		StateDir        string          `json:"state_dir"`
	}
	var a alias
	if err := json.Unmarshal(raw, &a); err != nil {
		return err
	}
	c.EnabledAccounts = a.EnabledAccounts
	c.Paused = a.Paused
	c.StateDir = a.StateDir
	d1, err := parseDurationJSON(a.RefreshInterval)
	if err != nil {
		return fmt.Errorf("refresh_interval: %w", err)
	}
	c.RefreshInterval = d1
	d2, err := parseDurationJSON(a.TriggerLeadTime)
	if err != nil {
		return fmt.Errorf("trigger_lead_time: %w", err)
	}
	c.TriggerLeadTime = d2
	d3, err := parseDurationJSON(a.RetryInterval)
	if err != nil {
		return fmt.Errorf("retry_interval: %w", err)
	}
	c.RetryInterval = d3
	return nil
}

// parseDurationJSON accepts a JSON string ("12h") or a JSON number (nanoseconds).
func parseDurationJSON(raw json.RawMessage) (time.Duration, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return 0, nil
	}
	// String form: "12h", "1h30m", etc.
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if s == "" {
			return 0, nil
		}
		d, err := time.ParseDuration(s)
		if err != nil {
			return 0, fmt.Errorf("parse %q: %w", s, err)
		}
		return d, nil
	}
	// Number form: nanoseconds (Go default).
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		return time.Duration(n), nil
	}
	return 0, fmt.Errorf("must be a duration string like \"12h\" or a nanosecond integer")
}

// allAccountsWildcard is the sole enabled_accounts entry meaning "every
// Codex account CPA knows about, including accounts added after startup".
// It is the default so a fresh install starts working with zero UI setup;
// operators who want an explicit subset list the auth indexes instead.
const allAccountsWildcard = "*"

// Defaults (fork): patrol every 6h, redeem 1h before expiry, retry every 10m.
const (
	defaultRefreshInterval = 6 * time.Hour
	defaultTriggerLeadTime = 1 * time.Hour
	defaultRetryInterval   = 10 * time.Minute

	// An automatic redeem must stay "about to expire": at most 2h early.
	maxTriggerLeadTime = 2 * time.Hour
	maxRetryInterval   = time.Hour
	maxRefreshInterval = 24 * time.Hour
)

// DefaultConfig returns the defaults: 6h patrol, 1h lead time, 10m retry, all
// accounts enabled via the wildcard. An explicitly empty list (config_yaml
// `enabled_accounts: []` or a UI save with nothing checked) soft-disables
// the plugin until accounts are opted back in.
func DefaultConfig() Config {
	return Config{
		RefreshInterval: defaultRefreshInterval,
		TriggerLeadTime: defaultTriggerLeadTime,
		RetryInterval:   defaultRetryInterval,
		EnabledAccounts: []string{allAccountsWildcard},
	}
}

// configEnablesAll reports whether the enabled list contains the all-accounts
// wildcard.
func configEnablesAll(enabled []string) bool {
	for _, a := range enabled {
		if a == allAccountsWildcard {
			return true
		}
	}
	return false
}

// parseConfigYAML parses user YAML and normalizes it. A config_yaml block
// that omits enabled_accounts inherits the all-accounts wildcard default;
// `enabled_accounts: []` is an explicit opt-out and stays empty. Malformed
// YAML returns an error; missing fields fall back to defaults via
// normalizeConfig.
func parseConfigYAML(raw []byte) (Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config yaml: %w", err)
	}
	if cfg.EnabledAccounts == nil {
		cfg.EnabledAccounts = []string{allAccountsWildcard}
	}
	return normalizeConfig(cfg), nil
}

// normalizeConfig applies floors and de-dup. Intervals below 1 minute are
// raised to 1 minute to prevent tight worker loops. Empty/duplicate account
// IDs are dropped. The all-accounts wildcard collapses over any explicit
// entries ("*" already includes them).
func normalizeConfig(cfg Config) Config {
	if cfg.RefreshInterval < time.Minute {
		cfg.RefreshInterval = defaultRefreshInterval
	}
	if cfg.TriggerLeadTime < time.Minute {
		cfg.TriggerLeadTime = defaultTriggerLeadTime
	}
	if cfg.RetryInterval < time.Minute {
		cfg.RetryInterval = defaultRetryInterval
	}
	// Upper bounds: an automatic redeem must stay "about to expire".
	if cfg.TriggerLeadTime > maxTriggerLeadTime {
		cfg.TriggerLeadTime = maxTriggerLeadTime
	}
	if cfg.RetryInterval > maxRetryInterval {
		cfg.RetryInterval = maxRetryInterval
	}
	if cfg.RefreshInterval > maxRefreshInterval {
		cfg.RefreshInterval = maxRefreshInterval
	}
	cfg.UserAgent = strings.TrimSpace(cfg.UserAgent)
	out := make([]string, 0, len(cfg.EnabledAccounts))
	seen := map[string]bool{}
	for _, a := range cfg.EnabledAccounts {
		a = strings.TrimSpace(a)
		if a == "" || seen[a] {
			continue
		}
		seen[a] = true
		out = append(out, a)
	}
	if configEnablesAll(out) {
		out = []string{allAccountsWildcard}
	}
	cfg.EnabledAccounts = out
	return cfg
}
