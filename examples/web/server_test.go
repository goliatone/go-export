package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/goliatone/go-export/examples/web/config"
	"github.com/goliatone/go-router"
	"github.com/gorilla/websocket"
)

func testApp(t *testing.T, notifications bool) *App {
	t.Helper()
	cfg := config.Defaults()
	cfg.Export.ArtifactDir = t.TempDir()
	cfg.Export.MaxRows = 100
	cfg.Export.Notifications.Enabled = notifications
	cfg.Export.Notifications.Channels = []string{"inbox"}
	app, err := NewApp(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Close() })
	return app
}

func TestFiberV3HTTPContracts(t *testing.T) {
	app := testApp(t, false)
	server, err := buildServer(app)
	if err != nil {
		t.Fatal(err)
	}
	app.SetupRoutes(server.Router())
	native := server.WrappedRouter()
	for _, tc := range []struct {
		path, contains string
		status         int
	}{
		{"/", "Go Export Demo", 200},
		{"/api/definitions", "users", 200},
		{"/exports/assets/pdf/jspdf.umd.min.js", "jsPDF", 200},
		{"/not-a-route", "", 404},
	} {
		t.Run(tc.path, func(t *testing.T) {
			res, err := native.Test(httptest.NewRequest("GET", tc.path, nil))
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatal(err)
			}
			if res.StatusCode != tc.status || !strings.Contains(string(body), tc.contains) {
				t.Fatalf("status=%d body=%.300s", res.StatusCode, body)
			}
		})
	}
	req := httptest.NewRequest("OPTIONS", "/admin/exports", nil)
	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "Content-Type,X-Idempotency-Key")
	res, err := native.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 204 || res.Header.Get("Access-Control-Allow-Origin") != "*" || !strings.Contains(res.Header.Get("Access-Control-Allow-Methods"), "POST") {
		t.Fatalf("CORS status=%d headers=%v", res.StatusCode, res.Header)
	}
	for _, format := range []string{"csv", "template"} {
		t.Run(format, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/admin/exports", strings.NewReader(`{"definition":"users","format":"`+format+`","delivery":"sync"}`))
			req.Header.Set("Content-Type", "application/json")
			res, err := native.Test(req)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatal(err)
			}
			if res.StatusCode != 200 || !strings.Contains(string(body), "alice@example.com") {
				t.Fatalf("export status=%d body=%.500s", res.StatusCode, body)
			}
			if format == "csv" && (!strings.HasPrefix(res.Header.Get("Content-Type"), "text/csv") || !strings.Contains(res.Header.Get("Content-Disposition"), "users-export.csv")) {
				t.Fatalf("download headers=%v", res.Header)
			}
		})
	}
}

func TestFiberV3InboxShutdown(t *testing.T) {
	app := testApp(t, true)
	server, err := buildServer(app)
	if err != nil {
		t.Fatal(err)
	}
	app.SetupRoutes(server.Router())
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serve(ctx, app, server, addr, 3*time.Second) }()
	defer func() {
		stop, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = app.InboxHub.Shutdown(stop)
		_ = server.Shutdown(stop)
	}()
	waitForServer(t, &http.Client{Timeout: time.Second}, "http://"+addr+"/api/definitions", done)
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws/inbox?user_id=reader", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var badge inboxRealtimeMessage
	if err := conn.ReadJSON(&badge); err != nil {
		t.Fatal(err)
	}
	if badge.Topic != "inbox.badge" {
		t.Fatalf("badge=%+v", badge)
	}
	if err := app.InboxHub.broadcastToUser("reader", inboxRealtimeMessage{Topic: "test.update", Payload: map[string]string{"message": "hello"}}); err != nil {
		t.Fatal(err)
	}
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var update inboxRealtimeMessage
	if err := json.Unmarshal(data, &update); err != nil || update.Topic != "test.update" {
		t.Fatalf("update=%s error=%v", data, err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("inbox shutdown did not finish")
	}
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("WebSocket remained open")
	}
	app.InboxHub.mu.RLock()
	remaining := len(app.InboxHub.clients)
	app.InboxHub.mu.RUnlock()
	if remaining != 0 {
		t.Fatalf("%d inbox clients remain", remaining)
	}
}

func TestFiberV3StartupFailure(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server, err := buildServer(nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := serve(ctx, nil, server, listener.Addr().String(), time.Second); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("startup error = %v", err)
	}
}

func TestFiberV3Shutdown(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "drain"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			server, err := buildServer(nil)
			if err != nil {
				t.Fatal(err)
			}
			started, release := make(chan struct{}), make(chan struct{})
			server.Router().Get("/slow", func(c router.Context) error { close(started); <-release; return c.SendString("complete") })
			server.Router().Get("/ready", func(c router.Context) error { return c.SendString("ready") })
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := listener.Addr().String()
			listener.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			timeout := 3 * time.Second
			if deadline {
				timeout = 100 * time.Millisecond
			}
			done := make(chan error, 1)
			go func() { done <- serve(ctx, nil, server, addr, timeout) }()
			released := false
			defer func() {
				if !released {
					close(release)
				}
				shutdown, stop := context.WithTimeout(context.Background(), time.Second)
				defer stop()
				_ = server.Shutdown(shutdown)
			}()
			client := &http.Client{Timeout: 3 * time.Second}
			waitForServer(t, client, "http://"+addr+"/ready", done)
			request := make(chan error, 1)
			go func() {
				res, err := client.Get("http://" + addr + "/slow")
				if err == nil {
					defer res.Body.Close()
					var body []byte
					body, err = io.ReadAll(res.Body)
					if err == nil && string(body) != "complete" {
						err = errors.New("incomplete response")
					}
				}
				request <- err
			}()
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("request did not start")
			}
			cancel()
			if deadline {
				select {
				case err := <-done:
					if err == nil {
						t.Fatal("expected shutdown deadline")
					}
				case <-time.After(2 * time.Second):
					t.Fatal("shutdown exceeded bound")
				}
			} else {
				select {
				case err := <-done:
					t.Fatalf("shutdown did not drain: %v", err)
				case <-time.After(50 * time.Millisecond):
				}
			}
			close(release)
			released = true
			if err := <-request; err != nil && !deadline {
				t.Fatal(err)
			}
			if !deadline {
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(4 * time.Second):
					t.Fatal("shutdown did not finish")
				}
			}
		})
	}
}

func waitForServer(t *testing.T, client *http.Client, url string, done <-chan error) {
	t.Helper()
	limit := time.After(5 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		res, err := client.Get(url)
		if err == nil {
			res.Body.Close()
			if res.StatusCode == 200 {
				return
			}
		}
		select {
		case err := <-done:
			t.Fatalf("server stopped: %v", err)
		case <-limit:
			t.Fatal("server not ready")
		case <-ticker.C:
		}
	}
}
