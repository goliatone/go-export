package export

import "testing"

func TestSelectDeliveryAutoThresholds(t *testing.T) {
	base := DeliveryPolicy{
		Default: DeliverySync,
		Thresholds: DeliveryThresholds{
			MaxRows: 10,
		},
	}

	def := ResolvedDefinition{}

	syncReq := ExportRequest{Delivery: DeliveryAuto, EstimatedRows: 5}
	if got := SelectDelivery(syncReq, def, base); got != DeliverySync {
		t.Fatalf("expected sync, got %s", got)
	}

	asyncReq := ExportRequest{Delivery: DeliveryAuto, EstimatedRows: 50}
	if got := SelectDelivery(asyncReq, def, base); got != DeliveryAsync {
		t.Fatalf("expected async, got %s", got)
	}
}

func TestSelectDeliveryDefinitionOverride(t *testing.T) {
	base := DeliveryPolicy{
		Default: DeliverySync,
		Thresholds: DeliveryThresholds{
			MaxRows: 10,
		},
	}

	def := ResolvedDefinition{
		ExportDefinition: ExportDefinition{
			DeliveryPolicy: &DeliveryPolicy{
				Thresholds: DeliveryThresholds{MaxRows: 100},
			},
		},
	}

	req := ExportRequest{Delivery: DeliveryAuto, EstimatedRows: 50}
	if got := SelectDelivery(req, def, base); got != DeliverySync {
		t.Fatalf("expected sync with override, got %s", got)
	}
}
