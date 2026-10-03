package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"

	"github.com/markschroedr/watcher-core/internal/config"
	"github.com/markschroedr/watcher-core/internal/daemon"
	"github.com/markschroedr/watcher-core/internal/db"
	"github.com/markschroedr/watcher-core/internal/delivery"
	"github.com/markschroedr/watcher-core/internal/judge"
	"github.com/markschroedr/watcher-core/internal/runner"
	"github.com/markschroedr/watcher-core/internal/service"
)

type validationError struct{ error }

func invalid(e error) error {
	if e == nil {
		return nil
	}
	return validationError{e}
}
func Main(args []string) int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	e := execute(ctx, args)
	if e == nil || errors.Is(e, context.Canceled) {
		return 0
	}
	fmt.Fprintln(os.Stderr, "watcher:", e)
	var v validationError
	if errors.As(e, &v) {
		return 2
	}
	return 1
}
func execute(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] == "--help" || args[0] == "help" {
		fmt.Fprintln(os.Stdout, "watcher — watch text, judge criteria, pull findings\n\nAgent/app: add remove apply enable disable status findings alerts ack wake judge presets\nOperator: watch daemon service check catalog\n\nUse <command> --help for fields. Global: --registry PATH --json --input-json JSON")
		return nil
	}
	name := args[0]
	input, e := find(name)
	if e != nil {
		return invalid(e)
	}
	registry := config.DefaultRegistry()
	machine := false
	rest := []string{}
	for i := 1; i < len(args); i++ {
		switch args[i] {
		case "--registry":
			if i+1 == len(args) {
				return invalid(fmt.Errorf("--registry needs a path"))
			}
			i++
			registry = args[i]
		case "--json":
			machine = true
		case "--help":
			fmt.Printf("%s\n", name)
			for _, f := range fields(reflect.TypeOf(input).Elem()) {
				fmt.Printf("  %s (%s)\n", f.Flag, f.Type)
			}
			fmt.Println("  --input-json JSON  --registry PATH  --json")
			return nil
		default:
			rest = append(rest, args[i])
		}
	}
	if e = Parse(input, rest); e != nil {
		return invalid(e)
	}
	registry, e = filepath.Abs(registry)
	if e != nil {
		return e
	}
	emit := func(value any) error {
		if machine {
			return json.NewEncoder(os.Stdout).Encode(value)
		}
		b, e := json.MarshalIndent(value, "", "  ")
		if e != nil {
			return e
		}
		fmt.Fprintln(os.Stdout, string(b))
		return nil
	}
	if name == "catalog" {
		if input.(*CatalogInput).TypeScript {
			fmt.Print(TypeScript(FullCatalog()))
			return nil
		}
		return emit(FullCatalog())
	}
	if name == "service" {
		v := input.(*ServiceInput)
		status, e := service.Manage(ctx, v.Action, registry, v.EnvFile)
		if e != nil {
			return e
		}
		return emit(ServiceResult{Status: status})
	}
	if name == "add" || name == "apply" || name == "remove" || name == "enable" || name == "disable" {
		e = config.Update(registry, func(c *config.Config) (string, config.Document, error) {
			switch v := input.(type) {
			case *AddInput:
				path, e := config.Fragment(registry, v.File)
				if e != nil {
					return "", config.Document{}, e
				}
				d := c.Files[path]
				d.Watches = append(d.Watches, v.Watch)
				return path, d, nil
			case *ApplyInput:
				path, e := config.Fragment(registry, v.File)
				if e != nil {
					return "", config.Document{}, e
				}
				d := c.Files[path]
				d.Watches = v.Watches
				return path, d, nil
			case *NameInput:
				path, ok := c.Origins[v.Name]
				if !ok {
					return "", config.Document{}, fmt.Errorf("unknown watch %q", v.Name)
				}
				d := c.Files[path]
				for i, w := range d.Watches {
					if w.Name == v.Name {
						if name == "remove" {
							d.Watches = append(d.Watches[:i], d.Watches[i+1:]...)
						} else {
							enabled := name == "enable"
							d.Watches[i].Enabled = &enabled
						}
						break
					}
				}
				return path, d, nil
			}
			panic("unhandled registry mutation")
		})
		if e != nil {
			return invalid(e)
		}
		return emit(MutationResult{OK: true})
	}
	c, e := config.Load(registry)
	if e != nil {
		return invalid(e)
	}
	if name == "presets" {
		return emit(c.Presets)
	}
	if name == "check" {
		n := 0
		for _, w := range c.Watches {
			if w.IsEnabled() {
				n++
			}
		}
		return emit(CheckResult{OK: true, Watches: len(c.Watches), Enabled: n})
	}
	if name == "daemon" {
		e = daemon.Run(ctx, registry)
		if e != nil {
			return e
		}
		return emit(MutationResult{OK: true})
	}
	if name == "judge" {
		v := input.(*JudgeInput)
		w := adhoc(v.Name, v.Preset, v.Criteria)
		if e = validateWatch(c, &w); e != nil {
			return invalid(e)
		}
		data, e := io.ReadAll(os.Stdin)
		if e != nil {
			return e
		}
		client := &judge.Client{Config: c}
		result, e := client.Judge(ctx, w, []judge.Turn{{Kind: "stream", Content: string(data)}})
		if e != nil {
			return e
		}
		fmt.Fprintf(os.Stderr, "[watcher] judge cost %s\n", db.JSON(result.Cost))
		return emit(result)
	}
	if name == "watch" {
		v := input.(*WatchInput)
		sources := 0
		if v.File != "" {
			sources++
		}
		if v.Cmd != "" {
			sources++
		}
		if v.Stdin {
			sources++
		}
		if sources != 1 {
			return invalid(fmt.Errorf("choose exactly one of --file, --cmd, --stdin"))
		}
		w := adhoc(v.Name, v.Preset, v.Criteria)
		w.Source = config.Source{Type: "stdin"}
		if v.File != "" {
			path, e := filepath.Abs(v.File)
			if e != nil {
				return e
			}
			w.Source = config.Source{Type: "file", Path: path, FromStart: v.FromStart}
		}
		if v.Cmd != "" {
			w.Source = config.Source{Type: "command", Command: []string{"/bin/sh", "-c", v.Cmd}}
		}
		if v.FromStart && v.File == "" {
			return invalid(fmt.Errorf("--from-start requires --file"))
		}
		w.Cadence = config.Cadence{Every: v.Every, Quiet: v.Quiet}
		w.Window = v.Window
		w.Oneshot = v.Oneshot
		budget := .50
		w.MaxCost = &budget
		if v.MaxCost != nil {
			w.MaxCost = v.MaxCost
		}
		if v.Notify {
			w.Actions = append(w.Actions, config.Action{Name: "notify", Kind: "notify", Description: "Show a desktop notification. Use only for findings that need attention now."})
		}
		if e = validateWatch(c, &w); e != nil {
			return invalid(e)
		}
		s, e := db.Open(":memory:")
		if e != nil {
			return e
		}
		defer s.Close()
		// The same runner and delivery owner; no persistent state or repeating alerts in foreground mode.
		child, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- delivery.Loop(child, s, registry) }()
		runErr := runner.Run(ctx, s, c, w, os.Stdout)
		cancel()
		deliveryErr := <-done
		if runErr != nil {
			return runErr
		}
		if deliveryErr != nil && !errors.Is(deliveryErr, context.Canceled) {
			return deliveryErr
		}
		return delivery.Drain(ctx, s, registry)
	}
	s, e := db.Open(runner.Path(registry))
	if e != nil {
		return e
	}
	defer s.Close()
	switch v := input.(type) {
	case *FindingsInput:
		if v.Since < 0 {
			return invalid(fmt.Errorf("since must be nonnegative"))
		}
		for _, l := range v.Labels {
			parts := strings.SplitN(l, "=", 2)
			if len(parts) != 2 || parts[0] == "" {
				return invalid(fmt.Errorf("label must be k=v"))
			}
		}
		rows, e := runner.Findings(s, v.Since, v.Watch, v.Labels)
		if e != nil {
			return e
		}
		return emit(rows)
	case *AckInput:
		if (v.ID == "") == (v.Watch == "") {
			return invalid(fmt.Errorf("choose an alert ID or --watch"))
		}
		ids, e := delivery.Ack(ctx, s, v.ID, v.Watch)
		if e != nil {
			return e
		}
		return emit(AckResult{Acknowledged: ids})
	case *WakeInput:
		if v.Name != "" {
			if _, ok := c.Origins[v.Name]; !ok {
				return invalid(fmt.Errorf("unknown watch %q", v.Name))
			}
		}
		e = s.Tx(func(conn *sql.Conn) error {
			return db.Exec(conn, `INSERT INTO wakes(watch,created_at) VALUES(?,?)`, v.Name, db.Now())
		})
		if e != nil {
			return e
		}
		return emit(MutationResult{OK: true})
	}
	switch name {
	case "status":
		result, e := runner.GetStatus(s, c)
		if e != nil {
			return e
		}
		return emit(result)
	case "alerts":
		result, e := delivery.Alerts(s)
		if e != nil {
			return e
		}
		return emit(result)
	}
	return fmt.Errorf("unimplemented command %s", name)
}
func adhoc(name, preset string, criteria []string) config.Watch {
	if name == "" {
		name = fmt.Sprintf("adhoc-%d", os.Getpid())
	}
	if preset == "" {
		preset = "eco"
	}
	cs := []config.Criterion{}
	for i, text := range criteria {
		id := fmt.Sprintf("c%d", i+1)
		before, after, found := strings.Cut(text, "=")
		if found && before != "" && after != "" && !strings.ContainsAny(before, " \t\n") {
			id = before
			text = after
		}
		cs = append(cs, config.Criterion{ID: id, Text: text})
	}
	return config.Watch{Name: name, Preset: preset, Source: config.Source{Type: "stdin"}, Criteria: cs, Actions: []config.Action{{Name: "report", Kind: "record", Description: "Report a finding for a matched criterion."}}}
}
func validateWatch(c *config.Config, w *config.Watch) error {
	one := &config.Config{Presets: c.Presets, Watches: []config.Watch{*w}}
	if e := one.Resolve(); e != nil {
		return e
	}
	*w = one.Watches[0]
	return nil
}
