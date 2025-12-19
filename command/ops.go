package command

import (
	"context"
	"encoding/json"
	"os"
	"strings"

	gcmd "github.com/goliatone/go-command"
	"github.com/goliatone/go-errors"
	"github.com/goliatone/go-export/export"
)

// BatchRequest describes a request for backfill/scheduled exports.
type BatchRequest struct {
	Actor   export.Actor         `json:"actor"`
	Request export.ExportRequest `json:"request"`
}

// BatchLoader loads batch requests from a source.
type BatchLoader func(ctx context.Context) ([]BatchRequest, error)

// BatchRequester schedules export requests.
type BatchRequester interface {
	RequestExport(ctx context.Context, actor export.Actor, req export.ExportRequest) (export.ExportRecord, error)
}

// BatchCommand wires CLI/Cron execution for batch exports.
type BatchCommand struct {
	requester  BatchRequester
	loader     BatchLoader
	cliConfig  gcmd.CLIConfig
	cronConfig gcmd.HandlerConfig
}

// BatchOption customizes batch commands.
type BatchOption func(*BatchCommand)

// WithBatchCLIConfig overrides CLI configuration.
func WithBatchCLIConfig(cfg gcmd.CLIConfig) BatchOption {
	return func(cmd *BatchCommand) {
		cmd.cliConfig = cfg
	}
}

// WithBatchCronConfig overrides cron configuration.
func WithBatchCronConfig(cfg gcmd.HandlerConfig) BatchOption {
	return func(cmd *BatchCommand) {
		cmd.cronConfig = cfg
	}
}

// NewBackfillCommand creates a backfill CLI/Cron command.
func NewBackfillCommand(requester BatchRequester, loader BatchLoader, opts ...BatchOption) *BatchCommand {
	cmd := &BatchCommand{
		requester: requester,
		loader:    loader,
		cliConfig: gcmd.CLIConfig{
			Path:        []string{"exports-backfill"},
			Description: "Run export backfills",
			Group:       "exports",
		},
		cronConfig: gcmd.HandlerConfig{Expression: "0 0 * * *"},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(cmd)
		}
	}
	return cmd
}

// NewScheduledExportsCommand creates a scheduled exports CLI/Cron command.
func NewScheduledExportsCommand(requester BatchRequester, loader BatchLoader, opts ...BatchOption) *BatchCommand {
	cmd := &BatchCommand{
		requester: requester,
		loader:    loader,
		cliConfig: gcmd.CLIConfig{
			Path:        []string{"exports-scheduled"},
			Description: "Run scheduled exports",
			Group:       "exports",
		},
		cronConfig: gcmd.HandlerConfig{Expression: "0 * * * *"},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(cmd)
		}
	}
	return cmd
}

// CronHandler executes scheduled batch exports.
func (c *BatchCommand) CronHandler() func() error {
	return func() error {
		_, err := c.run(context.Background(), "")
		return err
	}
}

// CronOptions returns cron configuration.
func (c *BatchCommand) CronOptions() gcmd.HandlerConfig {
	if c == nil {
		return gcmd.HandlerConfig{}
	}
	return c.cronConfig
}

// CLIHandler exposes the CLI handler.
func (c *BatchCommand) CLIHandler() any {
	return &batchCLI{cmd: c}
}

// CLIOptions returns CLI configuration.
func (c *BatchCommand) CLIOptions() gcmd.CLIConfig {
	if c == nil {
		return gcmd.CLIConfig{}
	}
	return c.cliConfig
}

func (c *BatchCommand) run(ctx context.Context, from string) (int, error) {
	if c == nil {
		return 0, errors.New("batch command is nil", errors.CategoryInternal).
			WithTextCode("BATCH_CMD_NIL")
	}
	if c.requester == nil {
		return 0, errors.New("batch requester is required", errors.CategoryValidation).
			WithTextCode("REQUESTER_REQUIRED")
	}

	requests, err := c.loadRequests(ctx, from)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, item := range requests {
		req := item.Request
		req.Delivery = export.DeliveryAsync
		req.Output = nil
		if _, err := c.requester.RequestExport(ctx, item.Actor, req); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func (c *BatchCommand) loadRequests(ctx context.Context, from string) ([]BatchRequest, error) {
	if strings.TrimSpace(from) != "" {
		return loadBatchRequestsFromFile(from)
	}
	if c.loader == nil {
		return nil, errors.New("batch loader not configured", errors.CategoryValidation).
			WithTextCode("LOADER_REQUIRED")
	}
	return c.loader(ctx)
}

type batchCLI struct {
	cmd  *BatchCommand
	From string `kong:"name='from',help='Path to JSON batch export requests'"`
}

func (c *batchCLI) Run() error {
	if c == nil || c.cmd == nil {
		return errors.New("batch command is required", errors.CategoryInternal).
			WithTextCode("BATCH_CMD_NIL")
	}
	_, err := c.cmd.run(context.Background(), c.From)
	return err
}

func loadBatchRequestsFromFile(path string) ([]BatchRequest, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Wrap(err, errors.CategoryExternal, "read batch file failed").
			WithTextCode("BATCH_FILE_READ")
	}

	var requests []BatchRequest
	if err := json.Unmarshal(content, &requests); err != nil {
		return nil, errors.Wrap(err, errors.CategoryValidation, "batch file invalid JSON").
			WithTextCode("BATCH_FILE_INVALID")
	}
	return requests, nil
}

// CLIHandler exposes cleanup via CLI.
func (h *CleanupExportsHandler) CLIHandler() any {
	return &cleanupCLI{handler: h}
}

// CLIOptions describes cleanup CLI metadata.
func (h *CleanupExportsHandler) CLIOptions() gcmd.CLIConfig {
	return gcmd.CLIConfig{
		Path:        []string{"exports-cleanup"},
		Description: "Remove expired export artifacts",
		Group:       "exports",
	}
}

type cleanupCLI struct {
	handler *CleanupExportsHandler
}

func (c *cleanupCLI) Run() error {
	if c == nil || c.handler == nil {
		return errors.New("cleanup handler is required", errors.CategoryInternal).
			WithTextCode("CLEANUP_HANDLER_REQUIRED")
	}
	return c.handler.Execute(context.Background(), CleanupExports{})
}
