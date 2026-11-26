package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/goliatone/go-router"
)

func serve(ctx context.Context, app *App, server router.Server[*fiber.App], addr string, timeout time.Duration) error {
	managed, ok := server.(router.ManagedServer)
	if !ok {
		return fmt.Errorf("managed hosting unavailable")
	}
	return serveManaged(ctx, app, managed, addr, timeout)
}

func serveManaged(ctx context.Context, app *App, server router.ManagedServer, addr string, timeout time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	options := router.ManagedOptions{Address: addr, Network: "tcp4", DrainTimeout: timeout}
	if app != nil {
		options.OnShutdown = app.Shutdown
		options.OnBound = func(_ context.Context, address string) error {
			url := "http://" + address
			app.baseURL.Store(&url)
			return nil
		}
	}
	run, err := server.StartManaged(context.WithoutCancel(ctx), options)
	if err != nil {
		return err
	}
	if app != nil {
		app.hostDone = run.Done()
	}
	serving := run.Serving()
	for {
		select {
		case <-serving:
			log.Printf("Export serving at http://%s", run.Address())
			serving = nil
			if app != nil {
				app.maybeSendDemoNotification(ctx)
			}
		case <-run.Done():
			return run.Wait(context.Background())
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			if err := run.Stop(shutdown); err != nil {
				return err
			}
			return run.Wait(shutdown)
		}
	}
}
