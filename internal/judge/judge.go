package judge

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/markschroedr/watcher-core/internal/config"
)

//go:embed screen.txt
var screen string

//go:embed confirm.txt
var confirm string

type Finding struct {
	Action    string `json:"action"`
	Criterion string `json:"criterion"`
	Summary   string `json:"summary"`
	Evidence  string `json:"evidence"`
	CallID    string `json:"call_id,omitempty"`
	Status    string `json:"status" jsonschema:"enum=ok,enum=rejected"`
}
type Turn struct {
	Kind    string            `json:"kind"`
	Content string            `json:"content,omitempty"`
	Items   []json.RawMessage `json:"items,omitempty"`
	Finding *Finding          `json:"finding,omitempty"`
	Outcome string            `json:"outcome,omitempty"`
}
type Usage struct {
	Input   int64 `json:"input_tokens"`
	Output  int64 `json:"output_tokens"`
	Details struct {
		Cached int64 `json:"cached_tokens"`
	} `json:"input_tokens_details"`
}
type Call struct {
	Phase     string   `json:"phase"`
	Model     string   `json:"model"`
	Tier      string   `json:"service_tier"`
	RequestID string   `json:"request_id"`
	Usage     Usage    `json:"usage"`
	Cost      *float64 `json:"cost_usd"`
	Error     string   `json:"error,omitempty"`
}
type Result struct {
	Findings []Finding `json:"findings"`
	Cost     *float64  `json:"cost_usd"`
	Calls    []Call    `json:"calls"`
	Turns    []Turn    `json:"-"`
	Budget   bool      `json:"budget_exhausted"`
}
type Client struct {
	Config       *config.Config
	Record       func(Call) error
	Spent        float64
	Calls        []Call
	unknownPrice bool
}
type response struct {
	ID     string            `json:"id"`
	Model  string            `json:"model"`
	Tier   string            `json:"service_tier"`
	Status string            `json:"status"`
	Usage  Usage             `json:"usage"`
	Output []json.RawMessage `json:"output"`
	Error  struct {
		Message string `json:"message"`
	} `json:"error"`
}

func (c *Client) call(ctx context.Context, p config.Preset, phase, watch, instructions string, input any, tools []any, choice any) (response, error) {
	var result response
	call := Call{Phase: phase, Model: p.Model, Tier: p.Tier}
	body := map[string]any{"model": p.Model, "store": false, "prompt_cache_key": "watcher:" + watch, "include": []string{"reasoning.encrypted_content"}, "instructions": instructions, "input": input, "tools": tools}
	if p.Effort != "" {
		body["reasoning"] = map[string]string{"effort": p.Effort}
	}
	if p.Tier != "" {
		body["service_tier"] = p.Tier
	}
	if choice != nil {
		body["tool_choice"] = choice
	}
	key := os.Getenv(p.APIKeyEnv)
	if key == "" {
		return result, fmt.Errorf("environment variable %s is not set", p.APIKeyEnv)
	}
	base := strings.TrimRight(p.BaseURL, "/")
	if base == "" {
		base = "https://api.openai.com/v1"
	}
	data, e := json.Marshal(body)
	if e != nil {
		return result, e
	}
	req, e := http.NewRequestWithContext(ctx, "POST", base+"/responses", bytes.NewReader(data))
	if e != nil {
		return result, e
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 15 * time.Minute}
	res, e := client.Do(req)
	if e == nil {
		call.RequestID = res.Header.Get("x-request-id")
		b, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		e = readErr
		if e == nil {
			e = json.Unmarshal(b, &result)
		}
		if e == nil && (res.StatusCode < 200 || res.StatusCode >= 300) {
			e = fmt.Errorf("responses failed (%d): %s", res.StatusCode, result.Error.Message)
		}
		call.Usage = result.Usage
		if result.ID != "" {
			call.RequestID = result.ID
		}
		if result.Model != "" {
			call.Model = result.Model
		}
		if result.Tier != "" {
			call.Tier = result.Tier
		}
		if e == nil && result.Status != "completed" {
			e = fmt.Errorf("response status %q: %s", result.Status, result.Error.Message)
		}
	}
	if p.Input != nil && p.Output != nil {
		cached := p.Input
		if p.Cached != nil {
			cached = p.Cached
		}
		factor := 1.
		if p.Tier == "flex" && call.Tier == "default" {
			factor = 2
		}
		if p.Tier == "priority" && (call.Tier == "default" || call.Tier == "auto") {
			factor = .5
		}
		cost := factor * (float64(max(call.Usage.Input-call.Usage.Details.Cached, 0))**p.Input + float64(call.Usage.Details.Cached)**cached + float64(call.Usage.Output)**p.Output) / 1e6
		call.Cost = &cost
		c.Spent += cost
	} else {
		c.unknownPrice = true
	}
	if e != nil {
		call.Error = e.Error()
	}
	c.Calls = append(c.Calls, call)
	if c.Record != nil {
		if recordErr := c.Record(call); recordErr != nil {
			return result, recordErr
		}
	}
	return result, e
}
func parameters(criteria []config.Criterion) map[string]any {
	ids := []string{}
	for _, v := range criteria {
		ids = append(ids, v.ID)
	}
	return map[string]any{"type": "object", "properties": map[string]any{"criterion": map[string]any{"type": "string", "enum": ids, "description": "id of the matched criterion"}, "summary": map[string]any{"type": "string", "description": "one-sentence finding"}, "evidence": map[string]any{"type": "string", "description": "the stream excerpt that triggered this"}}, "required": []string{"criterion", "summary", "evidence"}, "additionalProperties": false}
}
func tool(name, description string, schema map[string]any) any {
	return map[string]any{"type": "function", "name": name, "description": description, "strict": true, "parameters": schema}
}
func wire(turns []Turn) []any {
	items := []any{}
	for _, t := range turns {
		switch t.Kind {
		case "stream":
			items = append(items, map[string]string{"role": "user", "content": t.Content})
		case "text":
			items = append(items, map[string]string{"role": "assistant", "content": t.Content})
		case "raw":
			for _, raw := range t.Items {
				items = append(items, raw)
			}
		case "finding":
			f := t.Finding
			args, _ := json.Marshal(map[string]string{"criterion": f.Criterion, "summary": f.Summary, "evidence": f.Evidence})
			items = append(items, map[string]string{"type": "function_call", "call_id": f.CallID, "name": f.Action, "arguments": string(args)}, map[string]string{"type": "function_call_output", "call_id": f.CallID, "output": t.Outcome})
		}
	}
	return items
}
func evaluateOutput(res response, w config.Watch, turns []Turn, allowEscalate bool) ([]Finding, []Turn, error) {
	fs := []Finding{}
	reasoning := []json.RawMessage{}
	text := ""
	for _, raw := range res.Output {
		var item struct {
			Type      string `json:"type"`
			Name      string `json:"name"`
			CallID    string `json:"call_id"`
			Arguments string `json:"arguments"`
			Content   []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		}
		if e := json.Unmarshal(raw, &item); e != nil {
			return nil, turns, e
		}
		switch item.Type {
		case "reasoning":
			reasoning = append(reasoning, raw)
		case "message":
			for _, v := range item.Content {
				if v.Type == "output_text" {
					text += v.Text
				}
			}
		case "function_call":
			var args struct {
				Criterion string `json:"criterion"`
				Summary   string `json:"summary"`
				Evidence  string `json:"evidence"`
			}
			dec := json.NewDecoder(strings.NewReader(item.Arguments))
			dec.DisallowUnknownFields()
			if e := dec.Decode(&args); e != nil {
				return nil, turns, e
			}
			validAction := allowEscalate && item.Name == "escalate"
			for _, a := range w.Actions {
				validAction = validAction || a.Name == item.Name
			}
			validCriterion := false
			for _, cr := range w.Criteria {
				validCriterion = validCriterion || cr.ID == args.Criterion
			}
			if !validAction || !validCriterion || item.CallID == "" || args.Summary == "" || args.Evidence == "" {
				return nil, turns, fmt.Errorf("invalid finding tool call")
			}
			fs = append(fs, Finding{Action: item.Name, Criterion: args.Criterion, Summary: args.Summary, Evidence: args.Evidence, CallID: item.CallID, Status: "ok"})
		}
	}
	if len(fs) > 0 && len(reasoning) > 0 {
		turns = append(turns, Turn{Kind: "raw", Items: reasoning})
	}
	if len(fs) == 0 {
		if text == "" {
			text = "noop"
		}
		turns = append(turns, Turn{Kind: "text", Content: text})
	}
	return fs, turns, nil
}
func (c *Client) screen(ctx context.Context, w config.Watch, preset, phase string, turns []Turn, escalate bool) ([]Finding, []Turn, error) {
	lines := []string{}
	for _, cr := range w.Criteria {
		lines = append(lines, "- ["+cr.ID+"] "+cr.Text)
	}
	instructions := strings.NewReplacer("{watch}", w.Name, "{criteria}", strings.Join(lines, "\n")).Replace(screen)
	tools := []any{}
	schema := parameters(w.Criteria)
	for _, a := range w.Actions {
		tools = append(tools, tool(a.Name, a.Description, schema))
	}
	if escalate {
		tools = append(tools, tool("escalate", "Re-judge the current conversation with a stronger judge when the criterion needs deeper analysis.", schema))
	}
	res, e := c.call(ctx, c.Config.Presets[preset], phase, w.Name, instructions, wire(turns), tools, nil)
	if e != nil {
		return nil, turns, e
	}
	return evaluateOutput(res, w, turns, escalate)
}
func (c *Client) exhausted(w config.Watch) bool { return w.MaxCost != nil && c.Spent >= *w.MaxCost }
func (c *Client) verify(ctx context.Context, w config.Watch, f Finding, turns []Turn) (bool, string, error) {
	criterion := ""
	for _, cr := range w.Criteria {
		if cr.ID == f.Criterion {
			criterion = cr.Text
		}
	}
	instructions := strings.NewReplacer("{watch}", w.Name, "{criterion_id}", f.Criterion, "{criterion_description}", criterion, "{summary}", f.Summary, "{evidence}", f.Evidence).Replace(confirm)
	var b strings.Builder
	for _, t := range turns {
		if t.Kind == "stream" {
			b.WriteString(t.Content)
		}
	}
	window := Suffix(b.String(), 16384)
	if window == "" {
		window = "(no stream context)"
	}
	schema := map[string]any{"type": "object", "properties": map[string]any{"confirmed": map[string]any{"type": "boolean"}, "reason": map[string]any{"type": "string", "description": "one sentence"}}, "required": []string{"confirmed", "reason"}, "additionalProperties": false}
	res, e := c.call(ctx, c.Config.Presets[w.Confirm], "confirm", w.Name, instructions, window, []any{tool("verdict", "Deliver the verification verdict.", schema)}, map[string]string{"type": "function", "name": "verdict"})
	if e != nil {
		return false, "", e
	}
	for _, raw := range res.Output {
		var v struct {
			Type      string `json:"type"`
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		}
		if e = json.Unmarshal(raw, &v); e != nil {
			return false, "", e
		}
		if v.Type == "function_call" && v.Name == "verdict" {
			var verdict struct {
				Confirmed bool   `json:"confirmed"`
				Reason    string `json:"reason"`
			}
			if e = json.Unmarshal([]byte(v.Arguments), &verdict); e != nil {
				return false, "", e
			}
			return verdict.Confirmed, verdict.Reason, nil
		}
	}
	return false, "verifier returned no verdict", nil
}

// Judge makes all semantic decisions before the caller commits. It never delivers actions.
func (c *Client) Judge(ctx context.Context, w config.Watch, turns []Turn) (Result, error) {
	started := c.Spent
	result := Result{Findings: []Finding{}}
	callStart := len(c.Calls)
	deferCost := func() {
		if !c.unknownPrice {
			cost := c.Spent - started
			result.Cost = &cost
		}
		result.Calls = append([]Call{}, c.Calls[callStart:]...)
		result.Turns = turns
		result.Budget = c.exhausted(w)
	}
	fs, next, e := c.screen(ctx, w, w.Preset, "screen", turns, w.Escalate != "")
	turns = next
	if e != nil {
		deferCost()
		return result, e
	}
	// Complete each screen tool's replay group before another model sees the conversation.
	needsEscalation := false
	direct := []Finding{}
	for _, f := range fs {
		outcome := "pending judgment"
		if f.Action == "escalate" {
			needsEscalation = true
			outcome = "escalating"
		} else {
			direct = append(direct, f)
		}
		copy := f
		turns = append(turns, Turn{Kind: "finding", Finding: &copy, Outcome: outcome})
	}
	if needsEscalation && !c.exhausted(w) {
		var more []Finding
		more, turns, e = c.screen(ctx, w, w.Escalate, "escalate", turns, false)
		if e != nil {
			deferCost()
			return result, e
		}
		for _, f := range more {
			copy := f
			turns = append(turns, Turn{Kind: "finding", Finding: &copy, Outcome: "pending judgment"})
		}
		direct = append(direct, more...)
	}
	for _, f := range direct {
		command := false
		for _, a := range w.Actions {
			if a.Name == f.Action {
				command = a.Kind == "command"
			}
		}
		outcome := "ok"
		if command {
			if c.exhausted(w) {
				f.Status = "rejected"
				outcome = "rejected: budget exhausted before confirmation"
			} else {
				var confirmed bool
				var reason string
				confirmed, reason, e = c.verify(ctx, w, f, turns)
				if e != nil {
					deferCost()
					return result, e
				}
				if !confirmed {
					f.Status = "rejected"
					outcome = "rejected: " + reason
				}
			}
		}
		for i := range turns {
			if turns[i].Finding != nil && turns[i].Finding.CallID == f.CallID {
				turns[i].Outcome = outcome
			}
		}
		result.Findings = append(result.Findings, f)
	}
	deferCost()
	return result, nil
}
func Suffix(text string, n int) string {
	rs := []rune(text)
	if len(rs) > n {
		return string(rs[len(rs)-n:])
	}
	return text
}
func chars(t Turn) int {
	switch t.Kind {
	case "raw":
		n := 0
		for _, r := range t.Items {
			n += utf8.RuneCount(r)
		}
		return n
	case "finding":
		return utf8.RuneCountInString(t.Finding.Summary + t.Finding.Evidence)
	default:
		return utf8.RuneCountInString(t.Content)
	}
}
func Reanchor(turns []Turn, chunk string, window int) []Turn {
	total := utf8.RuneCountInString(chunk)
	for _, t := range turns {
		total += chars(t)
	}
	if total <= window {
		return append(turns, Turn{Kind: "stream", Content: chunk})
	}
	// Keep complete response groups: never orphan reasoning items or their function calls.
	start := len(turns)
	kept := 0
	for i := len(turns) - 1; i >= 0; i-- {
		kept += chars(turns[i])
		if kept > window/4 {
			break
		}
		if turns[i].Kind == "stream" {
			start = i
		}
	}
	suffix := append([]Turn{}, turns[start:]...)
	return append(suffix, Turn{Kind: "stream", Content: chunk})
}
