package main

import (
	"context"
	"encoding/json"
	"github.com/goliatone/go-router"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestManagedExportAndActualDownloadAddress(t *testing.T) {
	app := testApp(t, true)
	server, err := buildServer(app)
	require.NoError(t, err)
	app.SetupRoutes(server.Router())
	managed := server.(router.ManagedServer)
	observer := &observedServer{ManagedServer: managed, runs: make(chan router.ManagedRun, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serveManaged(ctx, app, observer, "127.0.0.1:0", 3*time.Second) }()
	run := <-observer.runs
	await(t, run.Serving())
	t.Cleanup(func() {
		cancel()
		wait, stop := context.WithTimeout(context.Background(), 4*time.Second)
		defer stop()
		_ = run.Stop(wait)
		_ = run.Wait(wait)
	})
	base := "http://" + run.Address()
	require.Equal(t, base, *app.baseURL.Load())
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	res, err := client.Post(base+"/admin/exports", "application/json", strings.NewReader(`{"definition":"users","format":"template","delivery":"async"}`))
	require.NoError(t, err)
	payload, err := io.ReadAll(res.Body)
	res.Body.Close()
	require.NoError(t, err)
	require.Equal(t, 202, res.StatusCode, string(payload))
	var queued struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(payload, &queued))
	require.NotEmpty(t, queued.ID)
	deadline := time.Now().Add(3 * time.Second)
	for {
		resp, e := client.Get(base + "/admin/exports/" + queued.ID)
		require.NoError(t, e)
		body, e := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.NoError(t, e)
		var status struct {
			State string `json:"state"`
		}
		require.NoError(t, json.Unmarshal(body, &status))
		if status.State == "completed" {
			break
		}
		require.NotEqual(t, "failed", status.State, string(body))
		require.True(t, time.Now().Before(deadline), string(body))
		time.Sleep(10 * time.Millisecond)
	}
	for _, suffix := range []string{"download", "preview"} {
		resp, e := client.Get(base + "/admin/exports/" + queued.ID + "/" + suffix)
		require.NoError(t, e)
		body, e := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.NoError(t, e)
		require.Equal(t, 200, resp.StatusCode, string(body))
		require.Contains(t, string(body), "alice@example.com")
	}
	cancel()
	require.NoError(t, receiveResult(t, done))
	require.NoError(t, app.Close())
}
