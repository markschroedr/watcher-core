package db

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

type Store struct{ DB *sql.DB }

func Open(path string) (*Store, error) {
	if path != ":memory:" {
		if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
			return nil, e
		}
	}
	d, e := sql.Open("sqlite", path)
	if e != nil {
		return nil, e
	}
	d.SetMaxOpenConns(1)
	s := &Store{DB: d}
	for _, q := range []string{"PRAGMA busy_timeout=10000", "PRAGMA journal_mode=WAL", "PRAGMA foreign_keys=ON", schema} {
		if _, e = d.Exec(q); e != nil {
			d.Close()
			return nil, e
		}
	}
	return s, nil
}
func (s *Store) Close() error { return s.DB.Close() }
func Now() float64            { return float64(time.Now().UnixNano()) / 1e9 }
func JSON(v any) string {
	b, e := json.Marshal(v)
	if e != nil {
		panic(e)
	}
	return string(b)
}
func Exec(c *sql.Conn, q string, args ...any) error {
	_, e := c.ExecContext(context.Background(), q, args...)
	return e
}
func (s *Store) Tx(fn func(*sql.Conn) error) error {
	c, e := s.DB.Conn(context.Background())
	if e != nil {
		return e
	}
	defer c.Close()
	if e = Exec(c, "BEGIN IMMEDIATE"); e != nil {
		return e
	}
	defer Exec(c, "ROLLBACK")
	if e = fn(c); e != nil {
		return e
	}
	return Exec(c, "COMMIT")
}
func Lock(path string) (*os.File, error) { return lock(path) }
func Conflict() error                    { return fmt.Errorf("judgment revision changed before commit") }
