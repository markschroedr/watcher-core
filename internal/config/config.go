package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"

	"gopkg.in/yaml.v3"
)

type Preset struct {
	Model     string   `json:"model" yaml:"model"`
	BaseURL   string   `json:"base_url,omitempty" yaml:"base_url,omitempty"`
	APIKeyEnv string   `json:"api_key_env" yaml:"api_key_env"`
	Effort    string   `json:"reasoning_effort,omitempty" yaml:"reasoning_effort,omitempty"`
	Tier      string   `json:"service_tier,omitempty" yaml:"service_tier,omitempty"`
	Input     *float64 `json:"price_input_per_mtok" yaml:"price_input_per_mtok" jsonschema:"nullable"`
	Cached    *float64 `json:"price_cached_input_per_mtok" yaml:"price_cached_input_per_mtok" jsonschema:"nullable"`
	Output    *float64 `json:"price_output_per_mtok" yaml:"price_output_per_mtok" jsonschema:"nullable"`
	Cadence   Cadence  `json:"cadence" yaml:"cadence"`
	Window    int      `json:"window_max_chars" yaml:"window_max_chars"`
}
type Cadence struct {
	Every float64 `json:"every_seconds,omitempty" yaml:"every_seconds,omitempty"`
	Quiet float64 `json:"quiet_seconds,omitempty" yaml:"quiet_seconds,omitempty"`
}
type Source struct {
	Type          string   `json:"type" yaml:"type" jsonschema:"enum=file,enum=command,enum=stdin"`
	Path          string   `json:"path,omitempty" yaml:"path,omitempty"`
	Command       []string `json:"command,omitempty" yaml:"command,omitempty"`
	FromStart     bool     `json:"from_start,omitempty" yaml:"from_start,omitempty"`
	StartInode    *int64   `json:"start_inode,omitempty" yaml:"start_inode,omitempty"`
	StartPosition *int64   `json:"start_position,omitempty" yaml:"start_position,omitempty"`
}
type Criterion struct {
	ID   string `json:"id" yaml:"id"`
	Text string `json:"text" yaml:"text"`
}
type Action struct {
	Name        string   `json:"name" yaml:"name"`
	Kind        string   `json:"kind" yaml:"kind" jsonschema:"enum=record,enum=notify,enum=command"`
	Description string   `json:"description" yaml:"description"`
	Repeat      float64  `json:"repeat_every_seconds,omitempty" yaml:"repeat_every_seconds,omitempty"`
	Command     []string `json:"command,omitempty" yaml:"command,omitempty"`
}
type Watch struct {
	Name       string            `json:"name" yaml:"name"`
	Enabled    *bool             `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	Generation *int64            `json:"generation,omitempty" yaml:"generation,omitempty"`
	Preset     string            `json:"preset,omitempty" yaml:"preset,omitempty"`
	Source     Source            `json:"source" yaml:"source"`
	Criteria   []Criterion       `json:"criteria" yaml:"criteria"`
	Actions    []Action          `json:"actions" yaml:"actions"`
	Escalate   string            `json:"escalate,omitempty" yaml:"escalate,omitempty"`
	Confirm    string            `json:"confirm,omitempty" yaml:"confirm,omitempty"`
	Cadence    Cadence           `json:"cadence,omitempty" yaml:"cadence,omitempty"`
	Window     int               `json:"window_max_chars,omitempty" yaml:"window_max_chars,omitempty"`
	Oneshot    bool              `json:"oneshot,omitempty" yaml:"oneshot,omitempty"`
	MaxCost    *float64          `json:"max_cost_usd,omitempty" yaml:"max_cost_usd,omitempty"`
	Labels     map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
}

func (w Watch) IsEnabled() bool { return w.Enabled == nil || *w.Enabled }
func (w Watch) SourceIdentity() string {
	s := w.Source
	s.FromStart = false
	s.StartInode = nil
	s.StartPosition = nil
	b, _ := json.Marshal(s)
	return string(b)
}

type Document struct {
	Presets map[string]Preset `json:"presets,omitempty" yaml:"presets,omitempty"`
	Watches []Watch           `json:"watches" yaml:"watches"`
}
type Config struct {
	Presets map[string]Preset
	Watches []Watch
	Files   map[string]Document
	Origins map[string]string
}

func number(n float64) *float64 { return &n }
func Builtins() map[string]Preset {
	makePreset := func(e, t string, every, in, cached, out float64) Preset {
		return Preset{Model: "gpt-6-luna", APIKeyEnv: "OPENAI_API_KEY", Effort: e, Tier: t, Input: number(in), Cached: number(cached), Output: number(out), Cadence: Cadence{Every: every}, Window: 65536}
	}
	return map[string]Preset{
		"eco":      makePreset("medium", "flex", 300, .05, .005, .25),
		"fast":     makePreset("low", "priority", 5, .2, .02, 1),
		"thorough": makePreset("high", "flex", 300, .05, .005, .25),
		// Standard short-context pricing verified 2026-10-03: https://developers.openai.com/api/docs/pricing
		"sol": {Model: "gpt-6.1-sol", APIKeyEnv: "OPENAI_API_KEY", Effort: "medium", Tier: "default", Input: number(2), Cached: number(.1), Output: number(10), Cadence: Cadence{Every: 300}, Window: 65536},
	}
}
func DefaultRegistry() string {
	h, _ := os.UserHomeDir()
	return filepath.Join(h, ".watcher", "watches.yaml")
}
func Paths(registry string) ([]string, error) {
	registry, e := filepath.Abs(registry)
	if e != nil {
		return nil, e
	}
	paths := []string{registry}
	for _, pattern := range []string{"*.yaml", "*.yml"} {
		more, e := filepath.Glob(filepath.Join(filepath.Dir(registry), "watches.d", pattern))
		if e != nil {
			return nil, e
		}
		paths = append(paths, more...)
	}
	sort.Strings(paths[1:])
	return paths, nil
}
func Signature(registry string) (string, error) {
	paths, e := Paths(registry)
	if e != nil {
		return "", e
	}
	h := sha256.New()
	for _, p := range paths {
		b, e := os.ReadFile(p)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return "", e
		}
		fmt.Fprintf(h, "%s\x00", p)
		h.Write(b)
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
func read(path string) (Document, error) {
	var d Document
	b, e := os.ReadFile(path)
	if os.IsNotExist(e) {
		return Document{Watches: []Watch{}}, nil
	}
	if e != nil {
		return d, e
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if e = dec.Decode(&d); e != nil && e != io.EOF {
		return d, fmt.Errorf("%s: %w", path, e)
	}
	var extra any
	if e = dec.Decode(&extra); e != io.EOF {
		return d, fmt.Errorf("%s: expected one YAML document", path)
	}
	return d, nil
}
func Load(registry string) (*Config, error) { return load(registry, "", Document{}) }
func load(registry, replacement string, document Document) (*Config, error) {
	paths, e := Paths(registry)
	if e != nil {
		return nil, e
	}
	if replacement != "" {
		found := false
		for _, p := range paths {
			found = found || p == replacement
		}
		if !found {
			paths = append(paths, replacement)
			sort.Strings(paths[1:])
		}
	}
	c := &Config{Presets: Builtins(), Files: map[string]Document{}, Origins: map[string]string{}}
	overrides := map[string]bool{}
	for _, p := range paths {
		d, e := read(p)
		if p == replacement {
			d = document
			e = nil
		}
		if e != nil {
			return nil, e
		}
		c.Files[p] = d
		for n, preset := range d.Presets {
			if overrides[n] {
				return nil, fmt.Errorf("duplicate preset override %q", n)
			}
			overrides[n] = true
			c.Presets[n] = preset
		}
		for _, original := range d.Watches {
			// Resolved paths must not mutate the canonical document's slices.
			encoded, _ := json.Marshal(original)
			var w Watch
			if e := json.Unmarshal(encoded, &w); e != nil {
				return nil, e
			}
			if _, ok := c.Origins[w.Name]; ok {
				return nil, fmt.Errorf("duplicate watch %q", w.Name)
			}
			c.Origins[w.Name] = p
			if w.Source.Path != "" && !filepath.IsAbs(w.Source.Path) {
				w.Source.Path = filepath.Join(filepath.Dir(p), w.Source.Path)
			}
			for i, a := range w.Actions {
				if len(a.Command) > 0 && strings.Contains(a.Command[0], "/") && !filepath.IsAbs(a.Command[0]) {
					w.Actions[i].Command[0] = filepath.Join(filepath.Dir(p), a.Command[0])
				}
			}
			if len(w.Source.Command) > 0 && strings.Contains(w.Source.Command[0], "/") && !filepath.IsAbs(w.Source.Command[0]) {
				w.Source.Command[0] = filepath.Join(filepath.Dir(p), w.Source.Command[0])
			}
			c.Watches = append(c.Watches, w)
		}
	}
	if e = c.Resolve(); e != nil {
		return nil, e
	}
	return c, nil
}
func finite(n float64) bool { return !math.IsNaN(n) && !math.IsInf(n, 0) }

var toolName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func (c *Config) Resolve() error {
	for n, p := range c.Presets {
		if n == "" || p.Model == "" || p.APIKeyEnv == "" || p.Window <= 0 || !finite(p.Cadence.Every) || p.Cadence.Every <= 0 || !finite(p.Cadence.Quiet) || p.Cadence.Quiet < 0 {
			return fmt.Errorf("invalid preset %q", n)
		}
		for _, v := range []*float64{p.Input, p.Cached, p.Output} {
			if v != nil && (!finite(*v) || *v < 0) {
				return fmt.Errorf("invalid prices for %q", n)
			}
		}
	}
	for i, w := range c.Watches {
		if w.Name == "" || w.Name == "." || w.Name == ".." || strings.ContainsAny(w.Name, "/\\\x00") {
			return fmt.Errorf("invalid watch name %q", w.Name)
		}
		if w.Generation != nil && *w.Generation <= 0 {
			return fmt.Errorf("%s: generation must be positive", w.Name)
		}
		if w.Preset == "" {
			w.Preset = "eco"
		}
		p, ok := c.Presets[w.Preset]
		if !ok {
			return fmt.Errorf("%s: unknown preset %q", w.Name, w.Preset)
		}
		if w.Preset == "sol" {
			return fmt.Errorf("%s: sol is for escalation or confirmation only", w.Name)
		}
		if w.Window == 0 {
			w.Window = p.Window
		}
		if w.Cadence.Every == 0 {
			w.Cadence.Every = p.Cadence.Every
		}
		if w.Window <= 0 || !finite(w.Cadence.Every) || w.Cadence.Every <= 0 || !finite(w.Cadence.Quiet) || w.Cadence.Quiet < 0 {
			return fmt.Errorf("%s: invalid window or cadence", w.Name)
		}
		s := w.Source
		if (s.Type != "file" && s.Type != "command" && s.Type != "stdin") || (s.Type == "file") != (s.Path != "") || (s.Type == "command") != (len(s.Command) > 0) || (s.Type != "file" && (s.FromStart || s.StartInode != nil || s.StartPosition != nil)) || (s.StartInode == nil) != (s.StartPosition == nil) {
			return fmt.Errorf("%s: invalid source", w.Name)
		}
		if s.StartInode != nil && (*s.StartInode <= 0 || *s.StartPosition < 0) {
			return fmt.Errorf("%s: invalid start cursor", w.Name)
		}
		ids := map[string]bool{}
		for _, v := range w.Criteria {
			if v.ID == "" || v.Text == "" || ids[v.ID] {
				return fmt.Errorf("%s: invalid or duplicate criterion", w.Name)
			}
			ids[v.ID] = true
		}
		if len(ids) == 0 || len(w.Actions) == 0 {
			return fmt.Errorf("%s: criteria and actions required", w.Name)
		}
		ids = map[string]bool{}
		command := false
		for _, a := range w.Actions {
			if !toolName.MatchString(a.Name) || a.Name == "escalate" || ids[a.Name] || a.Description == "" {
				return fmt.Errorf("%s: invalid or duplicate action", w.Name)
			}
			ids[a.Name] = true
			if a.Kind != "record" && a.Kind != "notify" && a.Kind != "command" {
				return fmt.Errorf("%s: unknown action kind %q", w.Name, a.Kind)
			}
			if (a.Kind == "command") != (len(a.Command) > 0) || !finite(a.Repeat) || a.Repeat < 0 || (a.Repeat > 0 && a.Kind != "notify") {
				return fmt.Errorf("%s: invalid action %s", w.Name, a.Name)
			}
			command = command || a.Kind == "command"
		}
		if command != (w.Confirm != "") {
			return fmt.Errorf("%s: confirm is required iff a command action exists", w.Name)
		}
		for _, n := range []string{w.Preset, w.Confirm, w.Escalate} {
			if n == "" {
				continue
			}
			preset, ok := c.Presets[n]
			if !ok {
				return fmt.Errorf("%s: unknown preset %q", w.Name, n)
			}
			if w.MaxCost != nil && (preset.Input == nil || preset.Output == nil) {
				return fmt.Errorf("%s: budget requires prices on %s", w.Name, n)
			}
		}
		if w.MaxCost != nil && (!finite(*w.MaxCost) || *w.MaxCost <= 0) {
			return fmt.Errorf("%s: max_cost_usd must be positive", w.Name)
		}
		enabled := w.IsEnabled()
		w.Enabled = &enabled
		c.Watches[i] = w
	}
	return nil
}
func (c *Config) Fingerprint(w Watch) string {
	ps := map[string]Preset{w.Preset: c.Presets[w.Preset]}
	for _, n := range []string{w.Escalate, w.Confirm} {
		if n != "" {
			ps[n] = c.Presets[n]
		}
	}
	b, _ := json.Marshal(struct {
		Watch   Watch
		Presets map[string]Preset
	}{w, ps})
	return fmt.Sprintf("%x", sha256.Sum256(b))
}
func Fragment(registry, name string) (string, error) {
	if filepath.Base(name) != name || name == "." || name == ".." || name == "" || !(strings.HasSuffix(name, ".yaml") || strings.HasSuffix(name, ".yml")) {
		return "", fmt.Errorf("file must be a fragment basename ending in .yaml or .yml")
	}
	r, e := filepath.Abs(registry)
	return filepath.Join(filepath.Dir(r), "watches.d", name), e
}

// Update serializes cooperating writers and validates the complete proposed registry before rename.
func Update(registry string, change func(*Config) (string, Document, error)) error {
	registry, e := filepath.Abs(registry)
	if e != nil {
		return e
	}
	dir := filepath.Dir(registry)
	if e = os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	lock, e := os.OpenFile(filepath.Join(dir, "registry.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); e != nil {
		return e
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	c, e := Load(registry)
	if e != nil {
		return e
	}
	path, d, e := change(c)
	if e != nil {
		return e
	}
	if _, e = load(registry, path, d); e != nil {
		return e
	}
	b, e := yaml.Marshal(d)
	if e != nil {
		return e
	}
	return AtomicWrite(path, b, 0600)
}
func AtomicWrite(path string, b []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(dir, ".watcher-*")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if e = f.Chmod(mode); e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	if e = os.Rename(f.Name(), path); e != nil {
		return e
	}
	d, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
