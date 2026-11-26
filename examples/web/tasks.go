package main

import (
	"context"
	"errors"
	"sync"
	"time"
)

// taskOwner owns the demo queue goroutines, including startup notifications.
type taskOwner struct {
	mu       sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
	active   int
	stopping bool
	done     chan struct{}
}

func newTaskOwner() *taskOwner {
	ctx, cancel := context.WithCancel(context.Background())
	return &taskOwner{ctx: ctx, cancel: cancel, done: make(chan struct{})}
}
func (o *taskOwner) start(timeout time.Duration, work func(context.Context)) error {
	o.mu.Lock()
	if o.stopping {
		o.mu.Unlock()
		return errors.New("export host is stopping")
	}
	o.active++
	o.mu.Unlock()
	go func() {
		defer func() {
			o.mu.Lock()
			defer o.mu.Unlock()
			o.active--
			if o.stopping && o.active == 0 {
				close(o.done)
			}
		}()
		ctx, cancel := context.WithTimeout(o.ctx, timeout)
		defer cancel()
		work(ctx)
	}()
	return nil
}
func (o *taskOwner) stop() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.stopping {
		o.stopping = true
		o.cancel()
		if o.active == 0 {
			close(o.done)
		}
	}
}
func (o *taskOwner) wait(ctx context.Context) error {
	select {
	case <-o.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Shutdown seals admission, interrupts sockets and joins all owned work.
func (a *App) Shutdown(ctx context.Context) error {
	if a == nil {
		return nil
	}
	a.tasks.stop()
	socketErr := a.InboxHub.Shutdown(ctx)
	return errors.Join(socketErr, a.tasks.wait(ctx))
}
