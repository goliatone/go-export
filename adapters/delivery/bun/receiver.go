package deliverybun

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"time"

	delivery "github.com/goliatone/go-export/adapters/delivery"
	"github.com/goliatone/go-export/export"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

type inboxRecord struct {
	bun.BaseModel `bun:"table:export_webhook_inbox"`
	Namespace     string    `bun:",pk"`
	Key           string    `bun:",pk"`
	Digest        string    `bun:",notnull"`
	ReceivedAt    time.Time `bun:",notnull"`
}

// Receiver commits its inbox receipt and application SQL effect together. Apply
// must use only the supplied tx, never external I/O, commit, or another database.
// Side effects outside that transaction require their own idempotency/outbox.
type Receiver struct{ DB *bun.DB }

func MigrateReceiver(ctx context.Context, db *bun.DB) error {
	if db == nil {
		return invalid("receiver database required")
	}
	_, e := db.NewCreateTable().Model((*inboxRecord)(nil)).IfNotExists().Exec(ctx)
	return e
}
func (r *Receiver) Receive(ctx context.Context, namespace, key, digest string, body json.RawMessage, apply func(context.Context, bun.Tx, json.RawMessage) error) error {
	if r == nil || r.DB == nil || apply == nil {
		return invalid("receiver database and transactional effect required")
	}
	if namespace == "" || len(namespace) > 512 || len(key) != 64 || len(digest) != 64 || delivery.PayloadDigest(body) != digest || !json.Valid(body) || len(body) > 1<<20 {
		return invalid("invalid webhook receipt identity or body")
	}
	if _, e := hex.DecodeString(key); e != nil {
		return invalid("invalid webhook receipt key")
	}
	return r.DB.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		row := inboxRecord{Namespace: namespace, Key: key, Digest: digest, ReceivedAt: time.Now().UTC()}
		res, e := tx.NewInsert().Model(&row).On("CONFLICT (namespace, key) DO NOTHING").Exec(ctx)
		if e != nil {
			return e
		}
		n, e := res.RowsAffected()
		if e != nil {
			return e
		}
		q := tx.NewSelect().Model(&row).WherePK()
		if r.DB.Dialect().Name() == dialect.PG {
			q = q.For("UPDATE")
		}
		if e = q.Scan(ctx); e != nil {
			return e
		}
		if row.Digest != digest {
			return invalid("webhook receipt payload conflict")
		}
		if n == 0 {
			return nil
		}
		return apply(ctx, tx, body)
	})
}

// ReceiverHandler authenticates sender scope through Namespace, then delegates
// the receipt/effect transaction. Namespace MUST verify the sender independently
// of untrusted payload/headers (e.g. mTLS or a verified host credential).
type ReceiverHandler struct {
	Receiver  *Receiver
	Namespace func(*http.Request) (string, error)
	Apply     func(context.Context, bun.Tx, json.RawMessage) error
}

func (h *ReceiverHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.Receiver == nil || h.Namespace == nil || h.Apply == nil {
		http.Error(w, "receiver unavailable", http.StatusServiceUnavailable)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	ns, e := h.Namespace(r)
	if e != nil || ns == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	body, e := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if e != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	key, digest := r.Header.Get(delivery.WebhookKeyHeader), r.Header.Get(delivery.WebhookDigestHeader)
	if e = h.Receiver.Receive(r.Context(), ns, key, digest, body, h.Apply); e != nil {
		// Never echo payloads, credentials, SQL errors or callback error strings.
		status := http.StatusServiceUnavailable
		if export.KindFromError(e) == export.KindValidation {
			status = http.StatusConflict
		}
		http.Error(w, "webhook receipt unavailable", status)
		return
	}
	w.Header().Set(delivery.WebhookKeyHeader, key)
	w.Header().Set(delivery.WebhookDigestHeader, digest)
	w.WriteHeader(http.StatusNoContent)
}
