package export

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type formatContext struct {
	locale   string
	location *time.Location
}

func newFormatContext(opts FormatOptions) (formatContext, error) {
	ctx := formatContext{locale: opts.Locale}
	if opts.Timezone != "" {
		loc, err := time.LoadLocation(opts.Timezone)
		if err != nil {
			return formatContext{}, NewError(KindValidation, "invalid timezone", err)
		}
		ctx.location = loc
	}
	return ctx, nil
}

func (fc formatContext) formatTextValue(col Column, value any) (string, error) {
	if value == nil {
		return "", nil
	}

	switch normalizeColumnType(col.Type) {
	case "bool", "boolean":
		if boolValue, ok := coerceBool(value); ok {
			if boolValue {
				return "true", nil
			}
			return "false", nil
		}
	case "int", "integer", "int64":
		if intValue, ok := coerceInt(value); ok {
			if col.Format.Number != "" {
				return fmt.Sprintf(col.Format.Number, intValue), nil
			}
			return strconv.FormatInt(intValue, 10), nil
		}
	case "float", "float64", "decimal", "number":
		if floatValue, ok := coerceFloat(value); ok {
			if col.Format.Number != "" {
				return fmt.Sprintf(col.Format.Number, floatValue), nil
			}
			return strconv.FormatFloat(floatValue, 'f', -1, 64), nil
		}
	case "date", "datetime", "time", "timestamp":
		if timeValue, ok := coerceTime(value); ok {
			timeValue = fc.applyTimezone(timeValue)
			layout := col.Format.Layout
			if layout == "" {
				layout = defaultTimeLayout(col.Type)
			}
			return timeValue.Format(layout), nil
		}
	}

	return stringify(value), nil
}

func (fc formatContext) formatJSONValue(col Column, value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch normalizeColumnType(col.Type) {
	case "string":
		return stringify(value), nil
	case "bool", "boolean":
		if boolValue, ok := coerceBool(value); ok {
			return boolValue, nil
		}
	case "int", "integer", "int64":
		if intValue, ok := coerceInt(value); ok {
			return intValue, nil
		}
	case "float", "float64", "decimal", "number":
		if floatValue, ok := coerceFloat(value); ok {
			return floatValue, nil
		}
	case "date", "datetime", "time", "timestamp":
		if timeValue, ok := coerceTime(value); ok {
			timeValue = fc.applyTimezone(timeValue)
			layout := col.Format.Layout
			if layout != "" || normalizeColumnType(col.Type) == "date" {
				if layout == "" {
					layout = defaultTimeLayout(col.Type)
				}
				return timeValue.Format(layout), nil
			}
			return timeValue, nil
		}
	}

	return value, nil
}

func (fc formatContext) applyTimezone(value time.Time) time.Time {
	if fc.location == nil {
		return value
	}
	return value.In(fc.location)
}

func normalizeColumnType(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func defaultTimeLayout(colType string) string {
	switch normalizeColumnType(colType) {
	case "date":
		return "2006-01-02"
	case "time":
		return "15:04:05"
	default:
		return time.RFC3339
	}
}

func coerceBool(value any) (bool, bool) {
	switch v := value.(type) {
	case bool:
		return v, true
	case string:
		parsed, err := strconv.ParseBool(v)
		if err == nil {
			return parsed, true
		}
	}
	return false, false
}

func coerceFloat(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint64:
		return float64(v), true
	case json.Number:
		parsed, err := v.Float64()
		if err == nil {
			return parsed, true
		}
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		if err == nil {
			return parsed, true
		}
	}
	return 0, false
}

func coerceInt(value any) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case uint:
		return int64(v), true
	case uint64:
		if v <= math.MaxInt64 {
			return int64(v), true
		}
	case float64:
		if math.Mod(v, 1) == 0 {
			return int64(v), true
		}
	case float32:
		if math.Mod(float64(v), 1) == 0 {
			return int64(v), true
		}
	case json.Number:
		if parsed, err := v.Int64(); err == nil {
			return parsed, true
		}
	case string:
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			return parsed, true
		}
	}
	return 0, false
}

func coerceTime(value any) (time.Time, bool) {
	switch v := value.(type) {
	case time.Time:
		return v, true
	case *time.Time:
		if v == nil {
			return time.Time{}, false
		}
		return *v, true
	case string:
		layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02", "2006-01-02 15:04:05"}
		for _, layout := range layouts {
			if parsed, err := time.Parse(layout, v); err == nil {
				return parsed, true
			}
		}
	}
	return time.Time{}, false
}
