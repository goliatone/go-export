package exportdelivery

import (
	"context"
	"testing"
	"time"

	"github.com/goliatone/go-export/export"
)

type captureRequester struct {
	count int
}

func (c *captureRequester) RequestDelivery(ctx context.Context, req Request) error {
	c.count++
	return nil
}

func TestScheduleCommand_RunHonorsLimits(t *testing.T) {
	requester := &captureRequester{}
	loader := func(ctx context.Context) ([]Request, error) {
		return []Request{{Actor: export.Actor{ID: "actor-1"}, Export: export.ExportRequest{Definition: "users", Format: export.FormatPDF}, Targets: []Target{{Kind: TargetEmail, Email: EmailTarget{To: []string{"demo@example.com"}}}}},
			{Actor: export.Actor{ID: "actor-2"}, Export: export.ExportRequest{Definition: "teams", Format: export.FormatPDF}, Targets: []Target{{Kind: TargetEmail, Email: EmailTarget{To: []string{"demo@example.com"}}}}}}, nil
	}

	cmd := NewScheduledDeliveriesCommand(requester, loader, WithScheduleLimits(ScheduleLimits{MaxRequests: 1, MinInterval: time.Millisecond}))
	cmd.sleep = func(time.Duration) {}

	count, err := cmd.run(context.Background(), "")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 request, got %d", count)
	}
	if requester.count != 1 {
		t.Fatalf("expected requester count 1, got %d", requester.count)
	}
}
