package export

import (
	"context"
	"testing"
	"time"
)

func TestRetentionRules_TTLByRule(t *testing.T) {
	policy := RetentionRules{
		DefaultTTL: time.Hour,
		Rules: []RetentionRule{
			{Definition: "users", Format: FormatCSV, Role: "admin", TTL: 2 * time.Hour},
			{Definition: "users", TTL: 30 * time.Minute},
		},
	}

	ttl, err := policy.TTL(context.Background(), Actor{Roles: []string{"admin"}}, ExportRequest{
		Format:     FormatCSV,
		Definition: "users",
	}, ResolvedDefinition{ExportDefinition: ExportDefinition{Name: "users"}})
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if ttl != 2*time.Hour {
		t.Fatalf("expected rule ttl, got %s", ttl)
	}
}

func TestRetentionRules_TTLFallbacks(t *testing.T) {
	policy := RetentionRules{
		DefaultTTL: time.Hour,
		ByDefinition: map[string]time.Duration{
			"users": 15 * time.Minute,
		},
		ByFormat: map[Format]time.Duration{
			FormatCSV: 20 * time.Minute,
		},
		ByRole: map[string]time.Duration{
			"viewer": 10 * time.Minute,
		},
	}

	ttl, err := policy.TTL(context.Background(), Actor{Roles: []string{"viewer"}}, ExportRequest{
		Format:     FormatCSV,
		Definition: "users",
	}, ResolvedDefinition{ExportDefinition: ExportDefinition{Name: "users"}})
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if ttl != 15*time.Minute {
		t.Fatalf("expected definition ttl, got %s", ttl)
	}

	ttl, err = policy.TTL(context.Background(), Actor{Roles: []string{"viewer"}}, ExportRequest{
		Format:     FormatCSV,
		Definition: "orders",
	}, ResolvedDefinition{ExportDefinition: ExportDefinition{Name: "orders"}})
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if ttl != 20*time.Minute {
		t.Fatalf("expected format ttl, got %s", ttl)
	}

	ttl, err = policy.TTL(context.Background(), Actor{Roles: []string{"viewer"}}, ExportRequest{
		Format:     FormatJSON,
		Definition: "orders",
	}, ResolvedDefinition{ExportDefinition: ExportDefinition{Name: "orders"}})
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if ttl != 10*time.Minute {
		t.Fatalf("expected role ttl, got %s", ttl)
	}
}
