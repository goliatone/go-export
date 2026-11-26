package main

import (
	"context"
	"errors"
	"github.com/goliatone/go-notifications/pkg/interfaces/broadcaster"
	"github.com/goliatone/go-notifications/pkg/privacy"
	"github.com/goliatone/go-router"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

type socketProbe struct {
	router.WebSocketContext
	writes, interrupts atomic.Int32
	fail               bool
	entered, release   chan struct{}
}

func (s *socketProbe) ConnectionID() string { return "probe" }
func (s *socketProbe) WriteJSON(any) error {
	s.writes.Add(1)
	if s.entered != nil {
		close(s.entered)
		<-s.release
	}
	if s.fail {
		return errors.New("write failed")
	}
	return nil
}
func (s *socketProbe) InterruptRead() error { s.interrupts.Add(1); return nil }
func TestInboxRetirementAndFailedWrite(t *testing.T) {
	h := newInboxHub(nil)
	s := &socketProbe{fail: true}
	c := h.Add("reader", s)
	require.NotNil(t, c)
	require.Error(t, h.broadcastToUser("reader", inboxRealtimeMessage{}))
	require.EqualValues(t, 1, s.interrupts.Load())
	require.Len(t, h.snapshot("reader"), 1)
	h.Remove("reader", s)
	h.readers.Done()
	require.NoError(t, c.writeJSON(nil))
	c.interrupt()
	require.EqualValues(t, 1, s.writes.Load())
	require.EqualValues(t, 1, s.interrupts.Load())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, h.Shutdown(ctx))
	require.Nil(t, h.Add("reader", s))
	require.NoError(t, h.Shutdown(ctx))
}
func TestInboxShutdownDeadlineDuringWrite(t *testing.T) {
	h := newInboxHub(nil)
	s := &socketProbe{entered: make(chan struct{}), release: make(chan struct{})}
	c := h.Add("reader", s)
	written := make(chan struct{})
	go func() { _ = c.writeJSON(nil); close(written) }()
	await(t, s.entered)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, h.Shutdown(ctx), context.DeadlineExceeded)
	close(s.release)
	await(t, written)
	h.Remove("reader", s)
	h.readers.Done()
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	require.NoError(t, h.Shutdown(ctx2))
}

func TestInboxPublishedPrivacyPayload(t *testing.T) {
	h := newInboxHub(nil)
	target, other := &socketProbe{}, &socketProbe{}
	h.Add("reader", target)
	h.Add("other", other)
	require.NoError(t, h.Broadcast(context.Background(), broadcaster.Event{Topic: "inbox.created", Payload: map[string]any{"subject_id": (privacy.DefaultPolicy{}).SafeSubjectID("reader"), "id": "notification"}}))
	require.EqualValues(t, 1, target.writes.Load())
	require.Zero(t, other.writes.Load())
	h.Remove("reader", target)
	h.Remove("other", other)
	h.readers.Done()
	h.readers.Done()
	require.NoError(t, h.Shutdown(context.Background()))
}
