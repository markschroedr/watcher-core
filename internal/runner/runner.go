package runner

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
	"unicode/utf8"

	"github.com/markschroedr/watcher-core/internal/config"
	"github.com/markschroedr/watcher-core/internal/db"
	"github.com/markschroedr/watcher-core/internal/judge"
	"github.com/markschroedr/watcher-core/internal/stream"
)

type Progress struct {
	Watch          string        `json:"watch"`
	Fingerprint    string        `json:"fingerprint"`
	SourceIdentity string        `json:"-"`
	Cursor         stream.Cursor `json:"cursor"`
	Turns          []judge.Turn  `json:"-"`
	Revision       int64         `json:"revision"`
	Spent          float64       `json:"spent_usd"`
	Terminal       string        `json:"terminal_reason,omitempty"`
	Generation     *int64        `json:"loaded_generation"`
	State          string        `json:"state"`
	Error          string        `json:"last_error,omitempty"`
	Heartbeat      float64       `json:"heartbeat"`
	WakeSeq        int64         `json:"-"`
}
type Finding struct {
	Seq        int64             `json:"seq"`
	ID         string            `json:"id"`
	Watch      string            `json:"watch"`
	Generation *int64            `json:"generation"`
	Criterion  string            `json:"criterion"`
	Summary    string            `json:"summary"`
	Evidence   string            `json:"evidence"`
	Action     string            `json:"action"`
	Status     string            `json:"status" jsonschema:"enum=ok,enum=rejected"`
	Labels     map[string]string `json:"labels"`
	ObservedAt float64           `json:"observed_at"`
}

func load(s *db.Store, name string) (Progress, error) {
	p := Progress{Watch: name, Turns: []judge.Turn{}}
	var cursor, turns string
	e := s.DB.QueryRow(`SELECT fingerprint,source_identity,cursor_json,turns_json,revision,spent_usd,terminal_reason,loaded_generation,state,last_error,heartbeat,wake_seq FROM progress WHERE watch=?`, name).Scan(&p.Fingerprint, &p.SourceIdentity, &cursor, &turns, &p.Revision, &p.Spent, &p.Terminal, &p.Generation, &p.State, &p.Error, &p.Heartbeat, &p.WakeSeq)
	if e != nil {
		return p, e
	}
	if e = json.Unmarshal([]byte(cursor), &p.Cursor); e != nil {
		return p, e
	}
	e = json.Unmarshal([]byte(turns), &p.Turns)
	return p, e
}
func prepare(s *db.Store, c *config.Config, w config.Watch) (Progress, error) {
	p, e := load(s, w.Name)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return p, e
	}
	fingerprint := c.Fingerprint(w)
	identity := w.SourceIdentity()
	if p.SourceIdentity != identity {
		p.Cursor = stream.Cursor{}
	}
	if p.Fingerprint != fingerprint {
		p.Turns = []judge.Turn{}
		p.Spent = 0
		p.Terminal = ""
		p.Revision++
	}
	p.Fingerprint = fingerprint
	p.SourceIdentity = identity
	p.Generation = w.Generation
	p.State = "running"
	p.Error = ""
	p.Heartbeat = db.Now()
	if p.Terminal != "" {
		p.State = "terminal"
	}
	e = s.Tx(func(conn *sql.Conn) error {
		return db.Exec(conn, `INSERT INTO progress(watch,fingerprint,source_identity,cursor_json,turns_json,revision,spent_usd,terminal_reason,loaded_generation,state,last_error,heartbeat) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(watch) DO UPDATE SET fingerprint=excluded.fingerprint,source_identity=excluded.source_identity,cursor_json=excluded.cursor_json,turns_json=excluded.turns_json,revision=excluded.revision,spent_usd=excluded.spent_usd,terminal_reason=excluded.terminal_reason,loaded_generation=excluded.loaded_generation,state=excluded.state,last_error=excluded.last_error,heartbeat=excluded.heartbeat`, w.Name, p.Fingerprint, p.SourceIdentity, db.JSON(p.Cursor), db.JSON(p.Turns), p.Revision, p.Spent, p.Terminal, p.Generation, p.State, p.Error, p.Heartbeat)
	})
	return p, e
}
func Record(s *db.Store, watch, fingerprint string, trimmed int) func(judge.Call) error {
	return func(call judge.Call) error {
		cost := 0.
		if call.Cost != nil {
			cost = *call.Cost
		}
		return s.Tx(func(conn *sql.Conn) error {
			if e := db.Exec(conn, `INSERT INTO judgments(watch,phase,model,service_tier,request_id,usage_json,cost_usd,error,trimmed_unjudged_chars,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, watch, call.Phase, call.Model, call.Tier, call.RequestID, db.JSON(call.Usage), cost, call.Error, trimmed, db.Now()); e != nil {
				return e
			}
			return db.Exec(conn, `UPDATE progress SET spent_usd=spent_usd+?,heartbeat=? WHERE watch=? AND fingerprint=?`, cost, db.Now(), watch, fingerprint)
		})
	}
}

// Commit is the only transition from unread stream data to findings. All writes share one transaction.
func Commit(s *db.Store, w config.Watch, p *Progress, cursor stream.Cursor, result judge.Result) ([]Finding, error) {
	committed := []Finding{}
	now := db.Now()
	terminal := ""
	if result.Budget {
		terminal = "budget"
	}
	for _, f := range result.Findings {
		if f.Status == "ok" && w.Oneshot && terminal == "" {
			terminal = "oneshot"
		}
	}
	e := s.Tx(func(conn *sql.Conn) error {
		res, e := conn.ExecContext(context.Background(), `UPDATE progress SET cursor_json=?,turns_json=?,revision=revision+1,terminal_reason=?,state=?,last_error='',heartbeat=?,wake_seq=? WHERE watch=? AND fingerprint=? AND revision=?`, db.JSON(cursor), db.JSON(result.Turns), terminal, map[bool]string{true: "terminal", false: "running"}[terminal != ""], now, p.WakeSeq, w.Name, p.Fingerprint, p.Revision)
		if e != nil {
			return e
		}
		n, e := res.RowsAffected()
		if e != nil {
			return e
		}
		if n != 1 {
			return db.Conflict()
		}
		for index, f := range result.Findings {
			// Exclude provider call IDs and generated wording: retries may produce different prose.
			identity := db.JSON(struct {
				Watch, Fingerprint string
				Revision           int64
				Cursor             stream.Cursor
				Index              int
			}{w.Name, p.Fingerprint, p.Revision, cursor, index})
			id := fmt.Sprintf("%x", sha256.Sum256([]byte(identity)))
			labels := w.Labels
			if labels == nil {
				labels = map[string]string{}
			}
			row, e := conn.ExecContext(context.Background(), `INSERT INTO findings(id,watch,generation,criterion,summary,evidence,action,status,labels_json,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, id, w.Name, w.Generation, f.Criterion, f.Summary, f.Evidence, f.Action, f.Status, db.JSON(labels), now)
			if e != nil {
				return e
			}
			seq, e := row.LastInsertId()
			if e != nil {
				return e
			}
			finding := Finding{Seq: seq, ID: id, Watch: w.Name, Generation: w.Generation, Criterion: f.Criterion, Summary: f.Summary, Evidence: f.Evidence, Action: f.Action, Status: f.Status, Labels: labels, ObservedAt: now}
			committed = append(committed, finding)
			if f.Status == "ok" {
				for _, a := range w.Actions {
					if a.Name == f.Action && a.Kind != "record" {
						if e = db.Exec(conn, `INSERT INTO deliveries(id,kind,action_json,next_at,repeat_seconds) VALUES(?,?,?,?,?)`, id, a.Kind, db.JSON(a), now, a.Repeat); e != nil {
							return e
						}
					}
				}
			}
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	p.Cursor = cursor
	p.Turns = result.Turns
	p.Revision++
	p.Terminal = terminal
	return committed, nil
}
func Run(ctx context.Context, s *db.Store, c *config.Config, w config.Watch, out io.Writer) (err error) {
	p, e := prepare(s, c, w)
	if e != nil {
		return e
	}
	if p.Terminal != "" {
		return nil
	}
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() {
		state := "stopped"
		detail := ""
		if err != nil && ctx.Err() == nil {
			state = "failed"
			detail = err.Error()
		}
		if p.Terminal != "" {
			state = "terminal"
		}
		saveErr := s.Tx(func(conn *sql.Conn) error {
			return db.Exec(conn, `UPDATE progress SET state=?,last_error=?,heartbeat=? WHERE watch=? AND fingerprint=?`, state, detail, db.Now(), w.Name, p.Fingerprint)
		})
		if err == nil {
			err = saveErr
		}
	}()
	if w.MaxCost != nil && p.Spent >= *w.MaxCost {
		p.Terminal = "budget"
		return s.Tx(func(conn *sql.Conn) error {
			return db.Exec(conn, `UPDATE progress SET terminal_reason='budget' WHERE watch=?`, w.Name)
		})
	}
	initial := func(cursor stream.Cursor) error {
		e := s.Tx(func(conn *sql.Conn) error {
			return db.Exec(conn, `UPDATE progress SET cursor_json=? WHERE watch=? AND fingerprint=? AND revision=?`, db.JSON(cursor), w.Name, p.Fingerprint, p.Revision)
		})
		return e
	}
	chunks := stream.Open(child, w.Source, w.Window, p.Cursor, initial)
	source := chunks
	defer func() {
		cancel()
		for range source {
		}
	}()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	buffer := ""
	cursor := p.Cursor
	lastJudge := time.Now()
	lastData := lastJudge
	lastHeartbeat := lastJudge
	retryAt := time.Time{}
	failures := 0
	trimmed := 0
	ended := false
	var sourceErr error
	wake := false
	wakeSeq := p.WakeSeq
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chunk, ok := <-chunks:
			if !ok {
				chunks = nil
				ended = true
			} else if chunk.End {
				ended = true
				sourceErr = chunk.Err
			} else {
				buffer += chunk.Text
				lastData = time.Now()
				cursor = chunk.Cursor
				n := utf8.RuneCountInString(buffer)
				if n > w.Window {
					trimmed += n - w.Window
					buffer = judge.Suffix(buffer, w.Window)
				}
			}
		case <-ticker.C:
		}
		now := time.Now()
		if now.Sub(lastHeartbeat) >= 2*time.Second {
			lastHeartbeat = now
			if e = s.Tx(func(conn *sql.Conn) error {
				return db.Exec(conn, `UPDATE progress SET heartbeat=? WHERE watch=? AND fingerprint=?`, db.Now(), w.Name, p.Fingerprint)
			}); e != nil {
				return e
			}
			e = s.DB.QueryRow(`SELECT coalesce(max(seq),0) FROM wakes WHERE watch='' OR watch=?`, w.Name).Scan(&wakeSeq)
			if e != nil {
				return e
			}
			wake = wakeSeq > p.WakeSeq
		}
		if buffer == "" {
			if ended {
				if sourceErr != nil {
					return sourceErr
				}
				return nil
			}
			continue
		}
		due := wake || ended || utf8.RuneCountInString(buffer) >= w.Window || now.Sub(lastJudge).Seconds() >= w.Cadence.Every || (w.Cadence.Quiet > 0 && now.Sub(lastData).Seconds() >= w.Cadence.Quiet)
		if !due || now.Before(retryAt) {
			continue
		}
		client := &judge.Client{Config: c, Spent: p.Spent, Record: Record(s, w.Name, p.Fingerprint, trimmed)}
		turns := judge.Reanchor(p.Turns, buffer, w.Window)
		result, judgeErr := client.Judge(ctx, w, turns)
		p.Spent = client.Spent
		fmt.Fprintf(os.Stderr, "[watcher] %s judgment cost $%.8f; spent $%.8f\n", w.Name, sumCost(result.Calls), p.Spent)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if judgeErr != nil {
			if w.MaxCost != nil && p.Spent >= *w.MaxCost {
				p.Terminal = "budget"
				return s.Tx(func(conn *sql.Conn) error {
					return db.Exec(conn, `UPDATE progress SET terminal_reason='budget',last_error=? WHERE watch=?`, judgeErr.Error(), w.Name)
				})
			}
			failures++
			delay := 5 * time.Second
			for i := 1; i < failures && delay < 300*time.Second; i++ {
				delay *= 2
			}
			if delay > 300*time.Second {
				delay = 300 * time.Second
			}
			retryAt = time.Now().Add(delay)
			fmt.Fprintf(os.Stderr, "[watcher] %s: %v; retry in %s\n", w.Name, judgeErr, delay)
			if e = s.Tx(func(conn *sql.Conn) error {
				return db.Exec(conn, `UPDATE progress SET last_error=? WHERE watch=?`, judgeErr.Error(), w.Name)
			}); e != nil {
				return e
			}
			continue
		}
		p.WakeSeq = wakeSeq
		rows, e := Commit(s, w, &p, cursor, result)
		if e != nil {
			return e
		}
		if out != nil {
			for _, f := range rows {
				if e = json.NewEncoder(out).Encode(f); e != nil {
					return e
				}
			}
		}
		buffer = ""
		trimmed = 0
		wake = false
		failures = 0
		retryAt = time.Time{}
		lastJudge = time.Now()
		if p.Terminal != "" {
			return nil
		}
		if ended {
			if sourceErr != nil {
				return sourceErr
			}
			return nil
		}
	}
}
func sumCost(calls []judge.Call) float64 {
	total := 0.
	for _, c := range calls {
		if c.Cost != nil {
			total += *c.Cost
		}
	}
	return total
}

func Findings(s *db.Store, since int64, watch string, labels []string) ([]Finding, error) {
	q := `SELECT seq,id,watch,generation,criterion,summary,evidence,action,status,labels_json,observed_at FROM findings WHERE seq>?`
	args := []any{since}
	if watch != "" {
		q += " AND watch=?"
		args = append(args, watch)
	}
	q += " ORDER BY seq"
	rows, e := s.DB.Query(q, args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Finding{}
	wanted := map[string]string{}
	for _, label := range labels {
		for i := 0; i < len(label); i++ {
			if label[i] == '=' {
				wanted[label[:i]] = label[i+1:]
				break
			}
		}
	}
	for rows.Next() {
		var f Finding
		var raw string
		if e = rows.Scan(&f.Seq, &f.ID, &f.Watch, &f.Generation, &f.Criterion, &f.Summary, &f.Evidence, &f.Action, &f.Status, &raw, &f.ObservedAt); e != nil {
			return nil, e
		}
		if e = json.Unmarshal([]byte(raw), &f.Labels); e != nil {
			return nil, e
		}
		matched := true
		for k, v := range wanted {
			actual, ok := f.Labels[k]
			if !ok || actual != v {
				matched = false
			}
		}
		if matched {
			out = append(out, f)
		}
	}
	return out, rows.Err()
}

type WatchStatus struct {
	Name             string  `json:"name"`
	Enabled          bool    `json:"enabled"`
	Generation       *int64  `json:"generation"`
	LoadedGeneration *int64  `json:"loaded_generation"`
	State            string  `json:"state"`
	Terminal         string  `json:"terminal_reason,omitempty"`
	Spent            float64 `json:"spent_usd"`
	Error            string  `json:"last_error,omitempty"`
	Heartbeat        float64 `json:"heartbeat"`
}
type Status struct {
	Alive         bool          `json:"daemon_alive"`
	PID           int           `json:"pid"`
	Watches       []WatchStatus `json:"watches"`
	PendingAlerts int           `json:"pending_alerts"`
}

func GetStatus(s *db.Store, c *config.Config) (Status, error) {
	out := Status{Watches: []WatchStatus{}}
	var heartbeat float64
	var alive bool
	e := s.DB.QueryRow(`SELECT pid,heartbeat,alive FROM daemon WHERE singleton=1`).Scan(&out.PID, &heartbeat, &alive)
	if e != nil && e != sql.ErrNoRows {
		return out, e
	}
	out.Alive = alive && db.Now()-heartbeat < 10
	for _, w := range c.Watches {
		p, e := load(s, w.Name)
		row := WatchStatus{Name: w.Name, Enabled: w.IsEnabled(), Generation: w.Generation, State: "pending"}
		if e == nil {
			row.LoadedGeneration = p.Generation
			row.State = p.State
			row.Terminal = p.Terminal
			row.Spent = p.Spent
			row.Error = p.Error
			row.Heartbeat = p.Heartbeat
			if !out.Alive && row.State == "running" {
				row.State = "stopped"
			}
		} else if e != sql.ErrNoRows {
			return out, e
		}
		out.Watches = append(out.Watches, row)
	}
	e = s.DB.QueryRow(`SELECT count(*) FROM deliveries WHERE kind='notify' AND repeat_seconds>0 AND acknowledged_at IS NULL AND cancelled_at IS NULL`).Scan(&out.PendingAlerts)
	return out, e
}
func Path(registry string) string { return filepath.Join(filepath.Dir(registry), "watcher.db") }
