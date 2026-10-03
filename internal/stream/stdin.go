package stream

import (
	"context"
	"os"
	"syscall"
)

// Own a duplicate descriptor so cancelling a watch does not close the caller's stdin.
func stdin(ctx context.Context, out chan<- Chunk) error {
	fd, e := syscall.Dup(int(os.Stdin.Fd()))
	if e != nil {
		return e
	}
	// NewFile joins Go's poller only for a nonblocking descriptor. Otherwise Close
	// cannot interrupt a Read on an idle pipe while its writer remains open.
	if e = syscall.SetNonblock(fd, true); e != nil {
		syscall.Close(fd)
		return e
	}
	f := os.NewFile(uintptr(fd), "watcher-stdin")
	defer f.Close()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			f.Close()
		case <-done:
		}
	}()
	return read(ctx, f, out)
}
