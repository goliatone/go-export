package exportjob

import (
	export "github.com/goliatone/go-export/export"
	job "github.com/goliatone/go-job"
)

// Envelope implements go-job envelope interfaces without depending on go-job structs.
type Envelope struct {
	Actor          export.Actor   `json:"actor,omitempty"`
	Scope          export.Scope   `json:"scope,omitempty"`
	Params         map[string]any `json:"params,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	RawBytes       int            `json:"-"`
}

var (
	_ job.EnvelopePayload          = (*Envelope)(nil)
	_ job.EnvelopeParamsSetter     = (*Envelope)(nil)
	_ job.EnvelopeRawContentSetter = (*Envelope)(nil)
)

// EnvelopeActor returns the actor metadata for codec usage.
func (e Envelope) EnvelopeActor() any { return e.Actor }

// EnvelopeScope returns the scope metadata for codec usage.
func (e Envelope) EnvelopeScope() any { return e.Scope }

// EnvelopeIdempotencyKey returns the idempotency key for codec usage.
func (e Envelope) EnvelopeIdempotencyKey() string { return e.IdempotencyKey }

// EnvelopeParams returns params for codec usage.
func (e Envelope) EnvelopeParams() map[string]any { return e.Params }

// SetEnvelopeParams applies sanitized params after decoding.
func (e *Envelope) SetEnvelopeParams(params map[string]any) {
	if e == nil {
		return
	}
	e.Params = params
}

// SetEnvelopeRawContentBytes captures decoded payload size.
func (e *Envelope) SetEnvelopeRawContentBytes(size int) {
	if e == nil {
		return
	}
	e.RawBytes = size
}
