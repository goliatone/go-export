package replaybun

import (
	"github.com/uptrace/bun"
	"time"
)

type replayTestRecord struct {
	bun.BaseModel `bun:"table:export_records,alias:export_records"`

	ID                     string    `bun:",pk"`
	Definition             string    `bun:",notnull"`
	Format                 string    `bun:",notnull"`
	State                  string    `bun:",notnull"`
	RequestedByID          string    `bun:"requested_by_id"`
	RequestedByTenantID    string    `bun:"requested_by_tenant_id"`
	RequestedByWorkspaceID string    `bun:"requested_by_workspace_id"`
	RequestedByRoles       []byte    `bun:"requested_by_roles"`
	RequestedByDetails     []byte    `bun:"requested_by_details"`
	ScopeTenantID          string    `bun:"scope_tenant_id"`
	ScopeWorkspaceID       string    `bun:"scope_workspace_id"`
	CountsProcessed        int64     `bun:"counts_processed"`
	CountsTotal            int64     `bun:"counts_total"`
	CountsErrors           int64     `bun:"counts_errors"`
	BytesWritten           int64     `bun:"bytes_written"`
	ArtifactKey            string    `bun:"artifact_key"`
	ArtifactMeta           []byte    `bun:"artifact_meta"`
	RequestPayload         []byte    `bun:"request_payload"`
	CreatedAt              time.Time `bun:"created_at"`
	StartedAt              time.Time `bun:"started_at,nullzero"`
	CompletedAt            time.Time `bun:"completed_at,nullzero"`
	ExpiresAt              time.Time `bun:"expires_at,nullzero"`
}
