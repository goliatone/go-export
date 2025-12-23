package exportdelivery

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"time"

	gcmd "github.com/goliatone/go-command"
	"github.com/goliatone/go-errors"
)

// ScheduleLoader loads scheduled delivery requests.
type ScheduleLoader func(ctx context.Context) ([]Request, error)

// ScheduleRequester enqueues scheduled delivery requests.
type ScheduleRequester interface {
	RequestDelivery(ctx context.Context, req Request) error
}

// ScheduleLimits bounds scheduled delivery execution.
type ScheduleLimits struct {
	MaxRequests int
	MinInterval time.Duration
}

// ScheduleCommand wires CLI/Cron execution for scheduled deliveries.
type ScheduleCommand struct {
	requester  ScheduleRequester
	loader     ScheduleLoader
	cliConfig  gcmd.CLIConfig
	cronConfig gcmd.HandlerConfig
	limits     ScheduleLimits
	sleep      func(time.Duration)
}

// ScheduleOption customizes scheduled delivery commands.
type ScheduleOption func(*ScheduleCommand)

// WithScheduleCLIConfig overrides CLI configuration.
func WithScheduleCLIConfig(cfg gcmd.CLIConfig) ScheduleOption {
	return func(cmd *ScheduleCommand) {
		cmd.cliConfig = cfg
	}
}

// WithScheduleCronConfig overrides cron configuration.
func WithScheduleCronConfig(cfg gcmd.HandlerConfig) ScheduleOption {
	return func(cmd *ScheduleCommand) {
		cmd.cronConfig = cfg
	}
}

// WithScheduleLimits overrides scheduling limits.
func WithScheduleLimits(limits ScheduleLimits) ScheduleOption {
	return func(cmd *ScheduleCommand) {
		cmd.limits = limits
	}
}

// NewScheduledDeliveriesCommand creates a scheduled delivery CLI/Cron command.
func NewScheduledDeliveriesCommand(requester ScheduleRequester, loader ScheduleLoader, opts ...ScheduleOption) *ScheduleCommand {
	cmd := &ScheduleCommand{
		requester: requester,
		loader:    loader,
		cliConfig: gcmd.CLIConfig{
			Path:        []string{"exports-deliver"},
			Description: "Run scheduled export deliveries",
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

// CronHandler executes scheduled deliveries.
func (c *ScheduleCommand) CronHandler() func() error {
	return func() error {
		_, err := c.run(context.Background(), "")
		return err
	}
}

// CronOptions returns cron configuration.
func (c *ScheduleCommand) CronOptions() gcmd.HandlerConfig {
	if c == nil {
		return gcmd.HandlerConfig{}
	}
	return c.cronConfig
}

// CLIHandler exposes the CLI handler.
func (c *ScheduleCommand) CLIHandler() any {
	return &scheduleCLI{cmd: c}
}

// CLIOptions returns CLI configuration.
func (c *ScheduleCommand) CLIOptions() gcmd.CLIConfig {
	if c == nil {
		return gcmd.CLIConfig{}
	}
	return c.cliConfig
}

func (c *ScheduleCommand) run(ctx context.Context, from string) (int, error) {
	if c == nil {
		return 0, errors.New("schedule command is nil", errors.CategoryInternal).
			WithTextCode("SCHEDULE_CMD_NIL")
	}
	if c.requester == nil {
		return 0, errors.New("schedule requester is required", errors.CategoryValidation).
			WithTextCode("REQUESTER_REQUIRED")
	}

	requests, err := c.loadRequests(ctx, from)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, req := range requests {
		if c.limits.MaxRequests > 0 && count >= c.limits.MaxRequests {
			break
		}
		if err := c.requester.RequestDelivery(ctx, req); err != nil {
			return count, err
		}
		count++
		if c.limits.MinInterval > 0 && c.sleep != nil {
			c.sleep(c.limits.MinInterval)
		}
	}
	return count, nil
}

func (c *ScheduleCommand) loadRequests(ctx context.Context, from string) ([]Request, error) {
	if strings.TrimSpace(from) != "" {
		return loadScheduleRequestsFromFile(from)
	}
	if c.loader == nil {
		return nil, errors.New("schedule loader not configured", errors.CategoryValidation).
			WithTextCode("LOADER_REQUIRED")
	}
	return c.loader(ctx)
}

type scheduleCLI struct {
	cmd  *ScheduleCommand
	From string `kong:"name='from',help='Path to JSON scheduled delivery requests'"`
}

func (c *scheduleCLI) Run() error {
	if c == nil || c.cmd == nil {
		return errors.New("schedule command is required", errors.CategoryInternal).
			WithTextCode("SCHEDULE_CMD_NIL")
	}
	_, err := c.cmd.run(context.Background(), c.From)
	return err
}

func loadScheduleRequestsFromFile(path string) ([]Request, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Wrap(err, errors.CategoryExternal, "read schedule file failed").
			WithTextCode("SCHEDULE_FILE_READ")
	}

	var requests []Request
	if err := json.Unmarshal(content, &requests); err != nil {
		return nil, errors.Wrap(err, errors.CategoryValidation, "schedule file invalid JSON").
			WithTextCode("SCHEDULE_FILE_INVALID")
	}
	return requests, nil
}
