package delivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/markschroedr/watcher-core/internal/config"
	"github.com/markschroedr/watcher-core/internal/db"
	"github.com/markschroedr/watcher-core/internal/runner"
	"github.com/markschroedr/watcher-core/internal/stream"
)

type Alert struct {
	ID          string   `json:"id"`
	Watch       string   `json:"watch"`
	Summary     string   `json:"summary"`
	Evidence    string   `json:"evidence"`
	CreatedAt   float64  `json:"created_at"`
	Repeat      float64  `json:"repeat_seconds"`
	NextAt      float64  `json:"next_at"`
	Attempts    int      `json:"attempts"`
	LastError   string   `json:"last_error"`
	DeliveredAt *float64 `json:"delivered_at"`
}

func Alerts(s *db.Store) ([]Alert, error) {
	rows, e := s.DB.Query(`SELECT d.id,f.watch,f.summary,f.evidence,f.observed_at,d.repeat_seconds,d.next_at,d.attempts,d.last_error,d.delivered_at FROM deliveries d JOIN findings f ON f.id=d.id WHERE d.kind='notify' AND d.repeat_seconds>0 AND d.acknowledged_at IS NULL AND d.cancelled_at IS NULL ORDER BY f.seq`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Alert{}
	for rows.Next() {
		var a Alert
		if e = rows.Scan(&a.ID, &a.Watch, &a.Summary, &a.Evidence, &a.CreatedAt, &a.Repeat, &a.NextAt, &a.Attempts, &a.LastError, &a.DeliveredAt); e != nil {
			return nil, e
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
func Ack(ctx context.Context, s *db.Store, id, watch string) ([]string, error) {
	ids := []string{}
	column, value := "d.id", id
	if watch != "" {
		column, value = "f.watch", watch
	}
	e := s.Tx(func(conn *sql.Conn) error {
		rows, e := conn.QueryContext(ctx, `SELECT d.id FROM deliveries d JOIN findings f ON f.id=d.id WHERE `+column+`=? AND d.kind='notify' AND d.repeat_seconds>0 AND d.acknowledged_at IS NULL AND d.cancelled_at IS NULL`, value)
		if e != nil {
			return e
		}
		for rows.Next() {
			var id string
			if e = rows.Scan(&id); e != nil {
				rows.Close()
				return e
			}
			ids = append(ids, id)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
		for _, id := range ids {
			if e = db.Exec(conn, `UPDATE deliveries SET acknowledged_at=? WHERE id=?`, db.Now(), id); e != nil {
				return e
			}
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	if runtime.GOOS == "darwin" {
		if _, e = exec.LookPath("terminal-notifier"); e == nil {
			for _, id := range ids {
				if e = stream.Execute(ctx, []string{"terminal-notifier", "-remove", id}, "", 10*time.Second); e != nil {
					fmt.Fprintln(os.Stderr, "[watcher] acknowledged but could not dismiss:", e)
				}
			}
		}
	}
	return ids, nil
}
func Reconcile(s *db.Store, c *config.Config) error {
	active := map[string]config.Action{}
	for _, w := range c.Watches {
		if w.IsEnabled() {
			for _, a := range w.Actions {
				if a.Kind == "notify" && a.Repeat > 0 {
					active[w.Name+"\x00"+a.Name] = a
				}
			}
		}
	}
	alerts, e := Alerts(s)
	if e != nil {
		return e
	}
	return s.Tx(func(conn *sql.Conn) error {
		for _, alert := range alerts {
			var name string
			if e := conn.QueryRowContext(context.Background(), `SELECT action FROM findings WHERE id=?`, alert.ID).Scan(&name); e != nil {
				return e
			}
			a, ok := active[alert.Watch+"\x00"+name]
			if !ok {
				if e := db.Exec(conn, `UPDATE deliveries SET cancelled_at=? WHERE id=?`, db.Now(), alert.ID); e != nil {
					return e
				}
			} else {
				if e := db.Exec(conn, `UPDATE deliveries SET repeat_seconds=? WHERE id=?`, a.Repeat, alert.ID); e != nil {
					return e
				}
			}
		}
		return nil
	})
}
func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
func Notify(ctx context.Context, f runner.Finding, registry string, repeat bool) error {
	summary := f.Summary
	title := "watcher: " + f.Watch
	if repeat {
		stamp := time.Unix(0, int64(f.ObservedAt*1e9)).In(time.Local).Format("Jan 02, 15:04 MST")
		summary = "Original alert at " + stamp + ":\n" + summary
	}
	var argv []string
	if runtime.GOOS == "darwin" {
		if _, e := exec.LookPath("terminal-notifier"); e == nil {
			argv = []string{"terminal-notifier", "-title", title, "-message", summary, "-sound", "Glass"}
			if repeat {
				binary, e := os.Executable()
				if e != nil {
					return e
				}
				ack := quote(binary) + " ack " + quote(f.ID) + " --registry " + quote(registry)
				argv = append(argv, "-subtitle", "Click to acknowledge and stop reminders", "-group", f.ID, "-execute", ack)
			}
		} else {
			if repeat {
				summary += "\nStop: watcher ack " + f.ID
			}
			message, _ := json.Marshal(summary)
			heading, _ := json.Marshal(title)
			argv = []string{"osascript", "-e", fmt.Sprintf("display notification %s with title %s sound name \"Glass\"", message, heading)}
		}
	} else if _, e := exec.LookPath("notify-send"); e == nil {
		if repeat {
			summary += "\nStop: watcher ack " + f.ID
		}
		argv = []string{"notify-send", title, summary}
	} else {
		return fmt.Errorf("no notification backend (terminal-notifier/osascript or notify-send)")
	}
	return stream.Execute(ctx, argv, "", 10*time.Second)
}
func Tick(ctx context.Context, s *db.Store, registry string) (bool, error) {
	var f runner.Finding
	var rawAction, labels, kind string
	var repeat float64
	e := s.DB.QueryRow(`SELECT f.seq,f.id,f.watch,f.generation,f.criterion,f.summary,f.evidence,f.action,f.status,f.labels_json,f.observed_at,d.kind,d.action_json,d.repeat_seconds FROM deliveries d JOIN findings f ON f.id=d.id WHERE d.acknowledged_at IS NULL AND d.cancelled_at IS NULL AND d.next_at<=? AND (d.delivered_at IS NULL OR d.repeat_seconds>0) ORDER BY d.next_at,f.seq LIMIT 1`, db.Now()).Scan(&f.Seq, &f.ID, &f.Watch, &f.Generation, &f.Criterion, &f.Summary, &f.Evidence, &f.Action, &f.Status, &labels, &f.ObservedAt, &kind, &rawAction, &repeat)
	if e == sql.ErrNoRows {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	var a config.Action
	if e = json.Unmarshal([]byte(rawAction), &a); e != nil {
		return false, e
	}
	if e = json.Unmarshal([]byte(labels), &f.Labels); e != nil {
		return false, e
	}
	// Recheck acknowledgement immediately before starting a potentially slow delivery.
	var pending bool
	if e = s.DB.QueryRow(`SELECT acknowledged_at IS NULL AND cancelled_at IS NULL FROM deliveries WHERE id=?`, f.ID).Scan(&pending); e != nil {
		return false, e
	}
	if !pending {
		return true, nil
	}
	if kind == "notify" {
		e = Notify(ctx, f, registry, repeat > 0)
	} else {
		e = stream.Execute(ctx, a.Command, db.JSON(f), 60*time.Second)
	}
	now := db.Now()
	detail := ""
	var delivered any = now
	next := now + repeat
	if e != nil {
		detail = e.Error()
		delivered = nil
		next = now + 5
		fmt.Fprintf(os.Stderr, "[watcher] delivery %s: %s\n", f.ID, detail)
	}
	saveErr := s.Tx(func(conn *sql.Conn) error {
		return db.Exec(conn, `UPDATE deliveries SET attempts=attempts+1,last_error=?,next_at=?,delivered_at=coalesce(?,delivered_at) WHERE id=? AND acknowledged_at IS NULL AND cancelled_at IS NULL`, detail, next, delivered, f.ID)
	})
	return true, saveErr
}
func Loop(ctx context.Context, s *db.Store, registry string) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		found, e := Tick(ctx, s, registry)
		if e != nil {
			return e
		}
		if !found {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}
}
func Drain(ctx context.Context, s *db.Store, registry string) error {
	// Foreground mode has no repeats. Try each queued action once; report failures rather than loop forever.
	var pending int
	if e := s.DB.QueryRow(`SELECT count(*) FROM deliveries WHERE delivered_at IS NULL AND acknowledged_at IS NULL AND cancelled_at IS NULL`).Scan(&pending); e != nil {
		return e
	}
	for i := 0; i < pending; i++ {
		_, e := Tick(ctx, s, registry)
		if e != nil {
			return e
		}
	}
	var failed int
	if e := s.DB.QueryRow(`SELECT count(*) FROM deliveries WHERE delivered_at IS NULL AND cancelled_at IS NULL AND acknowledged_at IS NULL`).Scan(&failed); e != nil {
		return e
	}
	if failed > 0 {
		return fmt.Errorf("%d foreground deliveries failed", failed)
	}
	return nil
}
