package command

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"time"

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

// BatchExecutor runs batch exports synchronously.
type BatchExecutor interface {
	ExecuteExport(ctx context.Context, actor export.Actor, req export.ExportRequest) (export.ExportRecord, error)
}

// BatchExecutorFunc adapts a function to a BatchExecutor.
type BatchExecutorFunc func(ctx context.Context, actor export.Actor, req export.ExportRequest) (export.ExportRecord, error)

func (f BatchExecutorFunc) ExecuteExport(ctx context.Context, actor export.Actor, req export.ExportRequest) (export.ExportRecord, error) {
	if f == nil {
		return export.ExportRecord{}, errors.New("batch executor is required", errors.CategoryInternal).
			WithTextCode("BATCH_EXECUTOR_NIL")
	}
	return f(ctx, actor, req)
}

// BatchCommand wires CLI/Cron execution for batch exports.
type BatchCommand struct {
	requester  BatchRequester
	executor   BatchExecutor
	loader     BatchLoader
	cliConfig  gcmd.CLIConfig
	cronConfig gcmd.HandlerConfig
	limits     BatchLimits
	sleep      func(time.Duration)
}

// BatchOption customizes batch commands.
type BatchOption func(*BatchCommand)

// BatchLimits bounds batch execution throughput.
type BatchLimits struct {
	MaxRequests int
	MinInterval time.Duration
}

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

// WithBatchLimits overrides batch execution limits.
func WithBatchLimits(limits BatchLimits) BatchOption {
	return func(cmd *BatchCommand) {
		cmd.limits = limits
	}
}

// WithBatchExecutor sets the synchronous executor for batch exports.
func WithBatchExecutor(executor BatchExecutor) BatchOption {
	return func(cmd *BatchCommand) {
		cmd.executor = executor
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
		sleep:      time.Sleep,
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
		sleep:      time.Sleep,
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
	if c.requester == nil && c.executor == nil {
		return 0, errors.New("batch requester or executor is required", errors.CategoryValidation).
			WithTextCode("REQUESTER_REQUIRED")
	}

	requests, err := c.loadRequests(ctx, from)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, item := range requests {
		if c.limits.MaxRequests > 0 && count >= c.limits.MaxRequests {
			break
		}
		req := item.Request
		req.Delivery = export.DeliveryAsync
		req.Output = nil
		if c.executor != nil {
			if _, err := c.executor.ExecuteExport(ctx, item.Actor, req); err != nil {
				return count, err
			}
		} else if _, err := c.requester.RequestExport(ctx, item.Actor, req); err != nil {
			return count, err
		}
		count++
		if c.limits.MinInterval > 0 && c.sleep != nil {
			c.sleep(c.limits.MinInterval)
		}
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

// DefinitionBatch builds PDF batch requests for a definition list.
type DefinitionBatch struct {
	Actor       export.Actor
	Definitions []string
	Request     export.ExportRequest
}

// BuildPDFBatchRequests returns async PDF export requests for each definition.
func BuildPDFBatchRequests(batch DefinitionBatch) []BatchRequest {
	if len(batch.Definitions) == 0 {
		return nil
	}
	req := batch.Request
	if req.Format == "" {
		req.Format = export.FormatPDF
	}

	requests := make([]BatchRequest, 0, len(batch.Definitions))
	for _, definition := range batch.Definitions {
		if strings.TrimSpace(definition) == "" {
			continue
		}
		item := BatchRequest{
			Actor: batch.Actor,
			Request: export.ExportRequest{
				Definition:        definition,
				SourceVariant:     req.SourceVariant,
				Format:            req.Format,
				Query:             req.Query,
				Selection:         req.Selection,
				Columns:           req.Columns,
				Locale:            req.Locale,
				Timezone:          req.Timezone,
				Delivery:          req.Delivery,
				IdempotencyKey:    req.IdempotencyKey,
				EstimatedRows:     req.EstimatedRows,
				EstimatedBytes:    req.EstimatedBytes,
				EstimatedDuration: req.EstimatedDuration,
				RenderOptions:     req.RenderOptions,
			},
		}
		requests = append(requests, item)
	}
	return requests
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
