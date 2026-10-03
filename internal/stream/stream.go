package stream

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/markschroedr/watcher-core/internal/config"
)

type Cursor struct {
	Inode    int64 `json:"inode,omitempty"`
	Position int64 `json:"position,omitempty"`
	Epoch    int64 `json:"epoch,omitempty"`
}
type Chunk struct {
	Text   string
	Cursor Cursor
	Err    error
	End    bool
}

func send(ctx context.Context, out chan<- Chunk, c Chunk) bool {
	select {
	case out <- c:
		return true
	case <-ctx.Done():
		return false
	}
}
func pause(ctx context.Context) bool {
	select {
	case <-time.After(200 * time.Millisecond):
		return true
	case <-ctx.Done():
		return false
	}
}
func Open(ctx context.Context, s config.Source, window int, cursor Cursor, initial func(Cursor) error) <-chan Chunk {
	out := make(chan Chunk, 1)
	go func() {
		defer close(out)
		var e error
		switch s.Type {
		case "file":
			e = tail(ctx, s, window, cursor, initial, out)
		case "command":
			e = command(ctx, s.Command, out)
		case "stdin":
			e = stdin(ctx, out)
		}
		if ctx.Err() == nil {
			send(ctx, out, Chunk{End: true, Err: e})
		}
	}()
	return out
}

// Incremental UTF-8: retain an incomplete suffix; replace malformed bytes, as Python's decoder does.
func decode(data []byte, final bool) (string, []byte) {
	var b strings.Builder
	i := 0
	for i < len(data) {
		if !final && !utf8.FullRune(data[i:]) {
			break
		}
		r, n := utf8.DecodeRune(data[i:])
		b.WriteRune(r)
		i += n
	}
	return b.String(), append([]byte(nil), data[i:]...)
}
func tail(ctx context.Context, s config.Source, window int, saved Cursor, initial func(Cursor) error, out chan<- Chunk) error {
	var f *os.File
	defer func() {
		if f != nil {
			f.Close()
		}
	}()
	first := true
	cursor := saved
	var pending []byte
	for ctx.Err() == nil {
		if f == nil {
			opened, e := os.Open(s.Path)
			if os.IsNotExist(e) {
				if !pause(ctx) {
					return ctx.Err()
				}
				continue
			}
			if e != nil {
				return e
			}
			f = opened
			st, e := f.Stat()
			if e != nil {
				return e
			}
			inode := int64(st.Sys().(*syscall.Stat_t).Ino)
			position := int64(0)
			if first {
				resumed := false
				if saved.Inode == inode && saved.Position >= 0 && saved.Position <= st.Size() {
					position = saved.Position
					resumed = true
				}
				if s.StartInode != nil && *s.StartInode == inode && *s.StartPosition <= st.Size() {
					if *s.StartPosition > position {
						position = *s.StartPosition
					}
					resumed = true
				}
				if !resumed {
					if !s.FromStart {
						position = st.Size()
					} else if st.Size() > int64(window) {
						position = st.Size() - int64(window)
					}
				}
			} else {
				cursor.Epoch++
			}
			cursor.Inode = inode
			cursor.Position = position
			pending = nil
			if _, e = f.Seek(position, io.SeekStart); e != nil {
				return e
			}
			if first && saved.Inode == 0 && initial != nil {
				if e = initial(cursor); e != nil {
					return e
				}
			}
			first = false
		}
		data := make([]byte, 65536)
		n, e := f.Read(data)
		if n > 0 {
			cursor.Position += int64(n)
			text, rest := decode(append(pending, data[:n]...), false)
			pending = rest
			snapshot := cursor
			snapshot.Position -= int64(len(pending))
			if text != "" && !send(ctx, out, Chunk{Text: text, Cursor: snapshot}) {
				return ctx.Err()
			}
			continue
		}
		if e != nil && e != io.EOF {
			return e
		}
		st, e := os.Stat(s.Path)
		if os.IsNotExist(e) {
			if !pause(ctx) {
				return ctx.Err()
			}
			continue
		}
		if e != nil {
			return e
		}
		rotated := int64(st.Sys().(*syscall.Stat_t).Ino) != cursor.Inode
		truncated := !rotated && st.Size() < cursor.Position
		if rotated || truncated {
			if len(pending) > 0 {
				text, _ := decode(pending, true)
				if !send(ctx, out, Chunk{Text: text, Cursor: cursor}) {
					return ctx.Err()
				}
			}
			pending = nil
			if rotated {
				f.Close()
				f = nil
			} else {
				cursor.Position = 0
				cursor.Epoch++
				if _, e = f.Seek(0, io.SeekStart); e != nil {
					return e
				}
			}
			continue
		}
		if !pause(ctx) {
			return ctx.Err()
		}
	}
	return ctx.Err()
}
func read(ctx context.Context, r io.Reader, out chan<- Chunk) error {
	var pending []byte
	for {
		data := make([]byte, 65536)
		n, e := r.Read(data)
		text, rest := decode(append(pending, data[:n]...), e != nil)
		pending = rest
		if text != "" && !send(ctx, out, Chunk{Text: text}) {
			return ctx.Err()
		}
		if e == io.EOF {
			return nil
		}
		if e != nil {
			return e
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}
func StopGroup(pid int) {
	if syscall.Kill(-pid, syscall.SIGTERM) != nil {
		return
	}
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		if syscall.Kill(-pid, 0) != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	syscall.Kill(-pid, syscall.SIGKILL)
}
func command(ctx context.Context, argv []string, out chan<- Chunk) error {
	r, w, e := os.Pipe()
	if e != nil {
		return e
	}
	defer r.Close()
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = w
	cmd.Stderr = w
	if e = cmd.Start(); e != nil {
		w.Close()
		return e
	}
	w.Close()
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	var once sync.Once
	stop := func() { once.Do(func() { StopGroup(cmd.Process.Pid); r.Close() }) }
	stopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			stop()
		case <-stopped:
		}
	}()
	readErr := read(ctx, r, out)
	stop()
	waitErr := <-waited
	close(stopped)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if readErr != nil {
		return readErr
	}
	if waitErr != nil {
		return fmt.Errorf("source command: %w", waitErr)
	}
	return nil
}

// Execute uses fixed argv, finding JSON on stdin, and a group-wide deadline.
func Execute(ctx context.Context, argv []string, input string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdin = strings.NewReader(input)
	cmd.Stdout = io.Discard
	cmd.Stderr = os.Stderr
	if e := cmd.Start(); e != nil {
		return e
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case e := <-done:
		StopGroup(cmd.Process.Pid)
		return e
	case <-ctx.Done():
		StopGroup(cmd.Process.Pid)
		<-done
		return ctx.Err()
	}
}
