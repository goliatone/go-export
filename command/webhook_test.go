package command

import (
	"context"
	"errors"
	delivery "github.com/goliatone/go-export/adapters/delivery"
	"github.com/goliatone/go-export/export"
	"testing"
)

type webhookCommands struct {
	n   int
	err error
}

func (s *webhookCommands) Schedule(_ context.Context, a export.Actor, r delivery.ScheduleWebhooksRequest) ([]delivery.WebhookReceipt, error) {
	s.n++
	return []delivery.WebhookReceipt{{ID: r.Key, ExportID: r.ExportID, TargetID: a.ID}}, s.err
}
func (s *webhookCommands) DeliverTarget(_ context.Context, a export.Actor, exportID, id string) error {
	s.n++
	return s.err
}
func TestWebhookCommandsDelegateAndValidate(t *testing.T) {
	s := &webhookCommands{}
	schedule := &ScheduleWebhooksHandler{Service: s}
	var receipts []delivery.WebhookReceipt
	m := ScheduleWebhooks{Actor: export.Actor{ID: "actor"}, Request: delivery.ScheduleWebhooksRequest{ExportID: "export", Key: "key"}, Result: &receipts}
	if e := schedule.Execute(context.Background(), m); e != nil || len(receipts) != 1 || receipts[0].ID != "key" {
		t.Fatal("schedule did not delegate", e, receipts)
	}
	deliver := &DeliverWebhookHandler{Service: s}
	if e := deliver.Execute(context.Background(), DeliverWebhook{}); e == nil || s.n != 1 {
		t.Fatal("invalid command delegated")
	}
	s.err = errors.New("service failure")
	if e := deliver.Execute(context.Background(), DeliverWebhook{Actor: m.Actor, ExportID: "export", ReceiptID: "id"}); !errors.Is(e, s.err) {
		t.Fatal("service error lost", e)
	}
	if m.Type() != ScheduleWebhooksCommand || (DeliverWebhook{}).Type() != DeliverWebhookCommand {
		t.Fatal("command identity changed")
	}
}
