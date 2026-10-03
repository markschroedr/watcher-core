package daemon

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/markschroedr/watcher-core/internal/config"
	"github.com/markschroedr/watcher-core/internal/db"
	"github.com/markschroedr/watcher-core/internal/delivery"
	"github.com/markschroedr/watcher-core/internal/runner"
)

type running struct {
	fingerprint string
	cancel      context.CancelFunc
	done        chan error
	finished    bool
	retry       time.Time
}

func Run(ctx context.Context, registry string) error {
	lock, e := db.Lock(filepath.Join(filepath.Dir(registry), "daemon.lock"))
	if e != nil {
		return fmt.Errorf("daemon lock: %w", e)
	}
	defer db.Unlock(lock)
	c, e := config.Load(registry)
	if e != nil {
		return e
	}
	s, e := db.Open(runner.Path(registry))
	if e != nil {
		return e
	}
	defer s.Close()
	sig, e := config.Signature(registry)
	if e != nil {
		return e
	}
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	tasks := map[string]*running{}
	deliveries := make(chan error, 1)
	go func() { deliveries <- delivery.Loop(child, s, registry) }()
	heartbeat := func(alive bool) error {
		return s.Tx(func(conn *sql.Conn) error {
			return db.Exec(conn, `INSERT INTO daemon(singleton,pid,heartbeat,alive) VALUES(1,?,?,?) ON CONFLICT(singleton) DO UPDATE SET pid=excluded.pid,heartbeat=excluded.heartbeat,alive=excluded.alive`, os.Getpid(), db.Now(), alive)
		})
	}
	defer func() {
		cancel()
		for _, t := range tasks {
			t.cancel()
			if !t.finished {
				<-t.done
			}
		}
		<-deliveries
		heartbeat(false)
	}()
	start := func(w config.Watch) {
		watchCtx, stop := context.WithCancel(child)
		t := &running{fingerprint: c.Fingerprint(w), cancel: stop, done: make(chan error, 1)}
		tasks[w.Name] = t
		loaded := c
		go func() { t.done <- runner.Run(watchCtx, s, loaded, w, nil) }()
	}
	sync := func() error {
		wanted := map[string]config.Watch{}
		for _, w := range c.Watches {
			if w.IsEnabled() {
				wanted[w.Name] = w
			}
		}
		for name, t := range tasks {
			w, ok := wanted[name]
			if !ok || t.fingerprint != c.Fingerprint(w) {
				t.cancel()
				if !t.finished {
					<-t.done
				}
				delete(tasks, name)
			}
		}
		for name, w := range wanted {
			if tasks[name] == nil {
				start(w)
			}
		}
		// Preserve old progress for inspection; disabled/removed runners are no longer live.
		return delivery.Reconcile(s, c)
	}
	if e = sync(); e != nil {
		return e
	}
	if e = heartbeat(true); e != nil {
		return e
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-deliveries:
			deliveries <- e
			return fmt.Errorf("delivery loop: %w", e)
		case <-ticker.C:
		}
		next, e := config.Signature(registry)
		if e != nil {
			fmt.Fprintln(os.Stderr, "[watcher] registry signature:", e)
		} else if next != sig {
			updated, e := config.Load(registry)
			if e != nil {
				fmt.Fprintln(os.Stderr, "[watcher] invalid registry, retaining loaded watches:", e)
			} else {
				c = updated
				sig = next
				if e = sync(); e != nil {
					return e
				}
			}
		}
		if e = heartbeat(true); e != nil {
			return e
		}
		for name, t := range tasks {
			if !t.finished {
				select {
				case e := <-t.done:
					t.finished = true
					var terminal string
					if queryErr := s.DB.QueryRow(`SELECT terminal_reason FROM progress WHERE watch=?`, name).Scan(&terminal); queryErr != nil {
						return queryErr
					}
					if terminal == "" {
						t.retry = time.Now().Add(30 * time.Second)
					}
					if e != nil {
						fmt.Fprintf(os.Stderr, "[watcher] %s stopped: %v\n", name, e)
					}
				default:
				}
			}
			if t.finished && !t.retry.IsZero() && !time.Now().Before(t.retry) {
				for _, w := range c.Watches {
					if w.Name == name && w.IsEnabled() {
						start(w)
						break
					}
				}
			}
		}
	}
}
