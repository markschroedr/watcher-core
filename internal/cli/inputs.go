package cli

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/markschroedr/watcher-core/internal/config"
)

type AddInput struct {
	File  string       `json:"file"`
	Watch config.Watch `json:"watch"`
}
type ApplyInput struct {
	File    string         `json:"file"`
	Watches []config.Watch `json:"watches"`
}
type NameInput struct {
	Name string `json:"name" cli:"pos"`
}
type EmptyInput struct{}
type FindingsInput struct {
	Since  int64    `json:"since,omitempty"`
	Watch  string   `json:"watch,omitempty"`
	Labels []string `json:"labels,omitempty" cli:"label"`
}
type AckInput struct {
	ID    string `json:"id,omitempty" cli:"pos"`
	Watch string `json:"watch,omitempty"`
}
type WakeInput struct {
	Name string `json:"name,omitempty" cli:"pos"`
}
type JudgeInput struct {
	Criteria []string `json:"criteria" cli:"criterion"`
	Preset   string   `json:"preset,omitempty"`
	Name     string   `json:"name,omitempty"`
}
type WatchInput struct {
	File      string   `json:"file,omitempty"`
	Cmd       string   `json:"cmd,omitempty"`
	Stdin     bool     `json:"stdin,omitempty"`
	Criteria  []string `json:"criteria" cli:"criterion"`
	Preset    string   `json:"preset,omitempty"`
	Name      string   `json:"name,omitempty"`
	Every     float64  `json:"every,omitempty"`
	Quiet     float64  `json:"quiet,omitempty"`
	Window    int      `json:"window,omitempty"`
	Oneshot   bool     `json:"oneshot,omitempty"`
	MaxCost   *float64 `json:"max_cost,omitempty"`
	Notify    bool     `json:"notify,omitempty"`
	FromStart bool     `json:"from_start,omitempty"`
}
type ServiceInput struct {
	Action  string `json:"action" cli:"pos" jsonschema:"enum=install,enum=uninstall,enum=status"`
	EnvFile string `json:"env_file,omitempty"`
}
type CatalogInput struct {
	TypeScript bool `json:"typescript,omitempty"`
}
type Definition struct {
	Name         string         `json:"name"`
	Description  string         `json:"description"`
	InputSchema  map[string]any `json:"inputSchema"`
	ResultSchema map[string]any `json:"resultSchema"`
	New          func() any     `json:"-"`
	Operator     bool           `json:"-"`
}

func definitions() []Definition {
	return []Definition{
		{Name: "add", Description: "Add one self-contained watch to a named YAML fragment. The daemon loads changes automatically.", New: func() any { return new(AddInput) }},
		{Name: "remove", Description: "Remove a watch by name from its defining YAML file.", New: func() any { return new(NameInput) }},
		{Name: "apply", Description: "Atomically replace the entire watch set in a named YAML fragment.", New: func() any { return new(ApplyInput) }},
		{Name: "enable", Description: "Enable a watch. An unchanged terminal watch stays terminal; change its generation to re-arm it.", New: func() any { return new(NameInput) }},
		{Name: "disable", Description: "Stop a watch and cancel its repeating alerts.", New: func() any { return new(NameInput) }},
		{Name: "status", Description: "Read daemon liveness and per-watch loaded generation, state, spend, and pending alerts.", New: func() any { return new(EmptyInput) }},
		{Name: "findings", Description: "Pull committed findings after an exclusive high-water sequence. Filter by watch or string labels.", New: func() any { return new(FindingsInput) }},
		{Name: "alerts", Description: "List unacknowledged repeating desktop alerts.", New: func() any { return new(EmptyInput) }},
		{Name: "ack", Description: "Stop repetitions for one alert id or all alerts for a watch.", New: func() any { return new(AckInput) }},
		{Name: "wake", Description: "Request judgment of buffered data for one watch or all watches. No data means no call.", New: func() any { return new(WakeInput) }},
		{Name: "judge", Description: "Stateless one-shot judgment of stdin against repeatable criteria; returns findings and token cost.", New: func() any { return new(JudgeInput) }},
		{Name: "presets", Description: "Read effective model presets, including YAML overrides.", New: func() any { return new(EmptyInput) }},
		{Name: "watch", Operator: true, New: func() any { return new(WatchInput) }},
		{Name: "daemon", Operator: true, New: func() any { return new(EmptyInput) }},
		{Name: "service", Operator: true, New: func() any { return new(ServiceInput) }},
		{Name: "check", Operator: true, New: func() any { return new(EmptyInput) }},
	}
}
func find(name string) (any, error) {
	for _, d := range definitions() {
		if d.Name == name {
			return d.New(), nil
		}
	}
	if name == "catalog" {
		return new(CatalogInput), nil
	}
	return nil, fmt.Errorf("unknown command %s", name)
}

// As in pd-memory, Go command fields own flags and the JSON boundary; nested values use JSON flags.
type field struct {
	Name string
	Type reflect.Type
	Pos  bool
	Flag string
}

func fields(t reflect.Type) []field {
	out := []field{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name := strings.Split(f.Tag.Get("json"), ",")[0]
		if name == "" || name == "-" {
			continue
		}
		tag := f.Tag.Get("cli")
		flag := strings.ReplaceAll(name, "_", "-")
		if tag != "" && tag != "pos" {
			flag = tag
		}
		out = append(out, field{name, f.Type, tag == "pos", "--" + flag})
	}
	return out
}
func Parse(input any, args []string) error {
	if len(args) == 2 && args[0] == "--input-json" {
		if e := ValidateJSON([]byte(args[1]), Schema(input)); e != nil {
			return e
		}
		return Decode([]byte(args[1]), input)
	}
	fs := fields(reflect.TypeOf(input).Elem())
	byFlag := map[string]field{}
	pos := []field{}
	for _, f := range fs {
		byFlag[f.Flag] = f
		if f.Pos {
			pos = append(pos, f)
		}
	}
	values := map[string]any{}
	pidx := 0
	for i := 0; i < len(args); i++ {
		arg := args[i]
		var f field
		raw := ""
		if !strings.HasPrefix(arg, "--") {
			if pidx >= len(pos) {
				return fmt.Errorf("unexpected argument %s", arg)
			}
			f = pos[pidx]
			pidx++
			raw = arg
		} else {
			var ok bool
			f, ok = byFlag[arg]
			if !ok {
				return fmt.Errorf("unknown argument %s", arg)
			}
			t := f.Type
			if t.Kind() == reflect.Pointer {
				t = t.Elem()
			}
			if t.Kind() == reflect.Bool {
				raw = "true"
				if i+1 < len(args) && (args[i+1] == "true" || args[i+1] == "false") {
					i++
					raw = args[i]
				}
			} else {
				if i+1 == len(args) {
					return fmt.Errorf("%s needs a value", arg)
				}
				i++
				raw = args[i]
			}
		}
		t := f.Type
		if t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		var v any
		var e error
		switch t.Kind() {
		case reflect.String:
			v = raw
		case reflect.Bool:
			v, e = strconv.ParseBool(raw)
		case reflect.Int, reflect.Int64:
			v, e = strconv.ParseInt(raw, 10, 64)
		case reflect.Float64:
			v, e = strconv.ParseFloat(raw, 64)
		case reflect.Slice:
			if t.Elem().Kind() == reflect.String {
				xs, _ := values[f.Name].([]string)
				values[f.Name] = append(xs, raw)
				continue
			}
			e = json.Unmarshal([]byte(raw), &v)
		case reflect.Struct, reflect.Map:
			e = json.Unmarshal([]byte(raw), &v)
		default:
			return fmt.Errorf("unsupported field %s", f.Name)
		}
		if e != nil {
			return e
		}
		if _, ok := values[f.Name]; ok {
			return fmt.Errorf("repeated argument %s", f.Name)
		}
		values[f.Name] = v
	}
	b, e := json.Marshal(values)
	if e != nil {
		return e
	}
	if e = ValidateJSON(b, Schema(input)); e != nil {
		return e
	}
	return Decode(b, input)
}
