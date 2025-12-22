package exportdelivery

import (
	"encoding/json"

	"github.com/goliatone/go-export/export"
	job "github.com/goliatone/go-job"
)

// Payload wraps a scheduled delivery request for job execution.
type Payload struct {
	Request Request `json:"request"`
}

func encodePayload(payload Payload) (json.RawMessage, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, export.NewError(export.KindValidation, "payload is not serializable", err)
	}
	return json.RawMessage(raw), nil
}

func decodePayload(msg *job.ExecutionMessage) (Payload, error) {
	if msg == nil || msg.Parameters == nil {
		return Payload{}, export.NewError(export.KindValidation, "job payload is required", nil)
	}

	raw, ok := msg.Parameters["payload"]
	if !ok {
		return Payload{}, export.NewError(export.KindValidation, "job payload missing", nil)
	}

	switch value := raw.(type) {
	case Payload:
		return value, nil
	case *Payload:
		if value == nil {
			return Payload{}, export.NewError(export.KindValidation, "job payload is nil", nil)
		}
		return *value, nil
	case json.RawMessage:
		return unmarshalPayload(value)
	case []byte:
		return unmarshalPayload(value)
	case string:
		return unmarshalPayload([]byte(value))
	default:
		data, err := json.Marshal(value)
		if err != nil {
			return Payload{}, export.NewError(export.KindValidation, "job payload is invalid", err)
		}
		return unmarshalPayload(data)
	}
}

func unmarshalPayload(data []byte) (Payload, error) {
	if len(data) == 0 {
		return Payload{}, export.NewError(export.KindValidation, "job payload is empty", nil)
	}
	var payload Payload
	if err := json.Unmarshal(data, &payload); err != nil {
		return Payload{}, export.NewError(export.KindValidation, "job payload is invalid", err)
	}
	return payload, nil
}
