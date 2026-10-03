package runner

import (
	"sync"
	"time"
	"unicode/utf8"

	"github.com/markschroedr/watcher-core/internal/judge"
	"github.com/markschroedr/watcher-core/internal/stream"
)

type batch struct {
	text     string
	cursor   stream.Cursor
	lastData time.Time
	trimmed  int
	ended    bool
	err      error
}

// Read ahead independently of model latency. A slow judgment must not block a
// watched command's stdout. The newest window wins; discarded data is accounted for.
type buffer struct {
	mu sync.Mutex
	batch
	window  int
	changed chan struct{}
	done    chan struct{}
}

func consume(source <-chan stream.Chunk, window int, cursor stream.Cursor) *buffer {
	b := &buffer{batch: batch{cursor: cursor, lastData: time.Now()}, window: window, changed: make(chan struct{}, 1), done: make(chan struct{})}
	go func() {
		defer close(b.done)
		for c := range source {
			b.mu.Lock()
			if c.End {
				b.ended = true
				b.err = c.Err
			} else {
				b.text += c.Text
				b.cursor = c.Cursor
				b.lastData = time.Now()
				b.trim()
			}
			b.mu.Unlock()
			b.signal()
		}
		b.mu.Lock()
		b.ended = true
		b.mu.Unlock()
		b.signal()
	}()
	return b
}
func (b *buffer) signal() {
	select {
	case b.changed <- struct{}{}:
	default:
	}
}
func (b *buffer) trim() {
	n := utf8.RuneCountInString(b.text)
	if n > b.window {
		b.trimmed += n - b.window
		b.text = judge.Suffix(b.text, b.window)
	}
}
func (b *buffer) peek() batch { b.mu.Lock(); defer b.mu.Unlock(); return b.batch }
func (b *buffer) take() batch {
	b.mu.Lock()
	defer b.mu.Unlock()
	part := b.batch
	b.text = ""
	b.trimmed = 0
	return part
}
func (b *buffer) restore(old batch) {
	b.mu.Lock()
	if b.text == "" {
		b.cursor = old.cursor
		b.lastData = old.lastData
	}
	b.text = old.text + b.text
	b.trimmed += old.trimmed
	b.trim()
	b.mu.Unlock()
	b.signal()
}
