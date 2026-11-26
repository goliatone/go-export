package main

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestAppShutdownJoinsOwnedWork(t *testing.T) {
	app := testApp(t, false)
	entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	require.NoError(t, app.tasks.start(time.Minute, func(ctx context.Context) { close(entered); <-ctx.Done(); close(cancelled); <-release }))
	await(t, entered)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, app.Shutdown(ctx), context.DeadlineExceeded)
	await(t, cancelled)
	require.Error(t, app.tasks.start(time.Second, func(context.Context) { t.Error("late work admitted") }))
	select {
	case <-app.tasks.done:
		t.Fatal("shutdown completed before worker joined")
	default:
	}
	// Deferred release runs before testApp cleanup, which must join the worker.
}
func TestTaskCompletionAndRepeatedShutdown(t *testing.T) {
	owner := newTaskOwner()
	finished := make(chan struct{})
	require.NoError(t, owner.start(time.Second, func(context.Context) { close(finished) }))
	await(t, finished)
	owner.stop()
	owner.stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, owner.wait(ctx))
	require.Error(t, owner.start(time.Second, func(context.Context) {}))
}

func TestCloseWaitsForNativeHandlers(t *testing.T) {
	app := testApp(t, false)
	joined := make(chan struct{})
	app.hostDone = joined
	done := make(chan error, 1)
	go func() { done <- app.Close() }()
	select {
	case err := <-done:
		close(joined)
		t.Fatalf("dependencies released before native handlers joined: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(joined)
	require.NoError(t, receiveResult(t, done))
}
