package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/goliatone/go-router"
	"github.com/stretchr/testify/require"
)

// Observe the actual production coordinator without replacing native hosting.
type observedServer struct {
	router.ManagedServer
	runs      chan router.ManagedRun
	configure func(*router.ManagedOptions)
}

func (s *observedServer) StartManaged(ctx context.Context, opts router.ManagedOptions) (router.ManagedRun, error) {
	if s.configure != nil {
		s.configure(&opts)
	}
	run, err := s.ManagedServer.StartManaged(ctx, opts)
	if err == nil {
		s.runs <- run
	}
	return run, err
}
func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for lifecycle boundary")
	}
}
func receiveResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(4 * time.Second):
		t.Fatal("coordinator did not finish")
		return nil
	}
}
func startObserved(t *testing.T, server router.ManagedServer, timeout time.Duration, configure func(*router.ManagedOptions)) (router.ManagedRun, context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	observed := &observedServer{ManagedServer: server, runs: make(chan router.ManagedRun, 1), configure: configure}
	done := make(chan error, 1)
	go func() { done <- serveManaged(ctx, nil, observed, "127.0.0.1:0", timeout) }()
	var run router.ManagedRun
	select {
	case run = <-observed.runs:
	case err := <-done:
		cancel()
		t.Fatalf("start: %v", err)
	case <-time.After(4 * time.Second):
		cancel()
		t.Fatal("start timed out")
	}
	t.Cleanup(func() {
		cancel()
		wait, stop := context.WithTimeout(context.Background(), 4*time.Second)
		defer stop()
		if err := run.Stop(wait); err != nil {
			t.Logf("cleanup stop: %v", err)
		}
		if err := run.Wait(wait); errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, router.ErrManagedDrainTimeout) {
			t.Errorf("managed workers did not join: %v", err)
		}
	})
	return run, cancel, done
}

func TestServeStartupFailure(t *testing.T) {
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { require.NoError(t, listener.Close()) }()
	server := testServer(t)
	result := make(chan error, 1)
	go func() {
		result <- serveManaged(context.Background(), nil, server, listener.Addr().String(), time.Second)
	}()
	err = receiveResult(t, result)
	var phase *router.ManagedError
	require.ErrorAs(t, err, &phase)
	require.Equal(t, "startup", phase.Phase)
}

func TestServePreCancellation(t *testing.T) {
	server := testServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, serveManaged(ctx, nil, server, "127.0.0.1:0", time.Second), context.Canceled)
	// The pre-cancelled call must not reserve a managed lifetime.
	run, cancel, done := startObserved(t, server, time.Second, nil)
	await(t, run.Serving())
	cancel()
	require.NoError(t, receiveResult(t, done))
}

func TestServeCancellationDuringStartup(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	run, cancel, done := startObserved(t, testServer(t), time.Second, func(o *router.ManagedOptions) {
		o.OnBound = func(context.Context, string) error { close(entered); <-release; return nil }
	})
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	await(t, entered)
	cancel()
	await(t, run.Stopping())
	select {
	case err := <-done:
		t.Fatalf("returned before startup joined: %v", err)
	default:
	}
	once.Do(func() { close(release) })
	require.NoError(t, receiveResult(t, done))
	await(t, run.Done())
	select {
	case <-run.Serving():
		t.Fatal("cancelled startup admitted requests")
	default:
	}
}

func TestServeOrdinaryRequestDrain(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "completion"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			server := testServer(t)
			entered, release, cancelled := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			server.Router().Get("/held", func(c router.Context) error {
				close(entered)
				if deadline {
					<-c.Context().Done()
					close(cancelled)
					<-release
					return nil
				}
				select {
				case <-release:
					return c.SendString("drained")
				case <-c.Context().Done():
					close(cancelled)
					return c.Context().Err()
				}
			})
			budget := 2 * time.Second
			if deadline {
				budget = 150 * time.Millisecond
			}
			run, cancel, done := startObserved(t, server, budget, nil)
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			await(t, run.Serving())
			client := &http.Client{Timeout: 4 * time.Second}
			response := make(chan error, 1)
			go func() {
				req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+run.Address()+"/held", nil)
				if err != nil {
					response <- err
					return
				}
				resp, err := client.Do(req)
				if err == nil {
					defer func() {
						if closeErr := resp.Body.Close(); closeErr != nil {
							t.Errorf("close response: %v", closeErr)
						}
					}()
					var body []byte
					body, err = io.ReadAll(resp.Body)
					if !deadline && (resp.StatusCode != http.StatusOK || string(body) != "drained") {
						err = errors.New("lost drained response")
					}
				}
				response <- err
			}()
			await(t, entered)
			cancel()
			await(t, run.Stopping())
			if deadline {
				err := receiveResult(t, done)
				require.ErrorIs(t, err, context.DeadlineExceeded)
				await(t, cancelled)
				once.Do(func() { close(release) })
				wait, stop := context.WithTimeout(context.Background(), 3*time.Second)
				defer stop()
				require.ErrorIs(t, run.Wait(wait), router.ErrManagedDrainTimeout)
				if err := receiveResult(t, response); err != nil {
					t.Logf("expected expired drain response: %v", err)
				}
			} else {
				select {
				case <-cancelled:
					t.Fatal("signal cancelled admitted request before drain")
				case err := <-done:
					t.Fatalf("returned before request drain: %v", err)
				default:
				}
				once.Do(func() { close(release) })
				require.NoError(t, receiveResult(t, response))
				require.NoError(t, receiveResult(t, done))
				await(t, run.Done())
			}
		})
	}
}

func TestServeShutdownFailure(t *testing.T) {
	failure := errors.New("shutdown failure")
	run, cancel, done := startObserved(t, testServer(t), time.Second, func(o *router.ManagedOptions) { o.OnShutdown = func(context.Context) error { return failure } })
	await(t, run.Serving())
	cancel()
	require.ErrorIs(t, receiveResult(t, done), failure)
}

type testHost interface {
	router.Server[*fiber.App]
	router.ManagedServer
}

func testServer(t *testing.T) testHost {
	t.Helper()
	s, err := buildServer(nil)
	require.NoError(t, err)
	managed, ok := s.(testHost)
	require.True(t, ok)
	return managed
}

func TestServeStreamDrain(t *testing.T) {
	server := testServer(t)
	entered, release, written := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	server.Router().Get("/stream", func(c router.Context) error {
		r, w := io.Pipe()
		go func() {
			defer close(written)
			defer w.Close()
			close(entered)
			<-release
			_, _ = io.WriteString(w, "complete streamed export")
		}()
		return c.SendStream(r)
	})
	run, cancel, done := startObserved(t, server, 2*time.Second, nil)
	await(t, run.Serving())
	response := make(chan error, 1)
	go func() {
		client := &http.Client{Timeout: 3 * time.Second}
		resp, err := client.Get("http://" + run.Address() + "/stream")
		if err == nil {
			defer resp.Body.Close()
			var body []byte
			body, err = io.ReadAll(resp.Body)
			if string(body) != "complete streamed export" {
				err = errors.New("stream truncated")
			}
		}
		response <- err
	}()
	await(t, entered)
	cancel()
	await(t, run.Stopping())
	select {
	case err := <-done:
		t.Fatalf("completed before stream drain: %v", err)
	default:
	}
	once.Do(func() { close(release) })
	require.NoError(t, receiveResult(t, response))
	require.NoError(t, receiveResult(t, done))
	await(t, written)
}
