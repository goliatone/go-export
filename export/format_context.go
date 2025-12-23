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
	ctx := formatContext{locale: strings.TrimSpace(opts.Locale)}
	if tz := strings.TrimSpace(opts.Timezone); tz != "" {
		loc, err := time.LoadLocation(tz)
		if err != nil {
			return formatContext{}, NewError(KindValidation, "invalid timezone", err)
		}
		ctx.location = loc
	}
	return ctx, nil
}

func (f formatContext) applyTimezone(value time.Time) time.Time {
	if f.location == nil {
		return value
	}
	return value.In(f.location)
}

func (f formatContext) formatTextValue(col Column, value any) (string, error) {
	if value == nil {
		return "", nil
	}
	switch normalizeColumnType(col.Type) {
	case "date", "datetime", "time":
		timeValue, ok := coerceTime(value)
		if !ok {
			return "", NewError(KindValidation, fmt.Sprintf("invalid time for column %q", col.Name), nil)
		}
		timeValue = f.applyTimezone(timeValue)
		layout := strings.TrimSpace(col.Format.Layout)
		if layout == "" {
			layout = defaultLayoutForType(normalizeColumnType(col.Type))
		}
		return timeValue.Format(layout), nil
	case "bool":
		boolValue, ok := coerceBool(value)
		if !ok {
			return "", NewError(KindValidation, fmt.Sprintf("invalid bool for column %q", col.Name), nil)
		}
		return strconv.FormatBool(boolValue), nil
	case "int":
		intValue, ok := coerceInt(value)
		if !ok {
			return "", NewError(KindValidation, fmt.Sprintf("invalid int for column %q", col.Name), nil)
		}
		return strconv.FormatInt(intValue, 10), nil
	case "float":
		floatValue, ok := coerceFloat(value)
		if !ok {
			return "", NewError(KindValidation, fmt.Sprintf("invalid number for column %q", col.Name), nil)
		}
		if format := strings.TrimSpace(col.Format.Number); format != "" && strings.Contains(format, "%") {
			return fmt.Sprintf(format, floatValue), nil
		}
		return strconv.FormatFloat(floatValue, 'f', -1, 64), nil
	default:
		return stringify(value), nil
	}
}

func (f formatContext) formatJSONValue(col Column, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	switch normalizeColumnType(col.Type) {
	case "date", "datetime", "time":
		timeValue, ok := coerceTime(value)
		if !ok {
			return nil, NewError(KindValidation, fmt.Sprintf("invalid time for column %q", col.Name), nil)
		}
		timeValue = f.applyTimezone(timeValue)
		layout := strings.TrimSpace(col.Format.Layout)
		if layout == "" {
			layout = defaultLayoutForType(normalizeColumnType(col.Type))
		}
		return timeValue.Format(layout), nil
	case "bool":
		boolValue, ok := coerceBool(value)
		if !ok {
			return nil, NewError(KindValidation, fmt.Sprintf("invalid bool for column %q", col.Name), nil)
		}
		return boolValue, nil
	case "int":
		intValue, ok := coerceInt(value)
		if !ok {
			return nil, NewError(KindValidation, fmt.Sprintf("invalid int for column %q", col.Name), nil)
		}
		return intValue, nil
	case "float":
		floatValue, ok := coerceFloat(value)
		if !ok {
			return nil, NewError(KindValidation, fmt.Sprintf("invalid number for column %q", col.Name), nil)
		}
		return floatValue, nil
	default:
		switch value.(type) {
		case string, bool, int, int64, float64, float32, json.Number:
			return value, nil
		default:
			return stringify(value), nil
		}
	}
}

func defaultLayoutForType(colType string) string {
	switch colType {
	case "date":
		return "2006-01-02"
	case "time":
		return "15:04:05"
	default:
		return time.RFC3339
	}
}

func normalizeColumnType(raw string) string {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	switch normalized {
	case "", "string", "text", "varchar", "uuid":
		return "string"
	case "bool", "boolean":
		return "bool"
	case "int", "integer", "int64", "int32", "int16", "int8", "bigint", "smallint":
		return "int"
	case "float", "float64", "float32", "decimal", "number", "numeric", "double":
		return "float"
	case "date":
		return "date"
	case "time", "timetz":
		return "time"
	case "datetime", "timestamp", "timestamptz":
		return "datetime"
	default:
		return normalized
	}
}

func coerceBool(value any) (bool, bool) {
	switch v := value.(type) {
	case bool:
		return v, true
	case *bool:
		if v == nil {
			return false, false
		}
		return *v, true
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(v))
		if err != nil {
			return false, false
		}
		return parsed, true
	case int:
		return v != 0, true
	case int64:
		return v != 0, true
	case int32:
		return v != 0, true
	case float64:
		return v != 0, true
	case float32:
		return v != 0, true
	case json.Number:
		parsed, err := v.Int64()
		if err == nil {
			return parsed != 0, true
		}
		floatValue, err := v.Float64()
		if err != nil {
			return false, false
		}
		return floatValue != 0, true
	default:
		return false, false
	}
}

func coerceInt(value any) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case int16:
		return int64(v), true
	case int8:
		return int64(v), true
	case uint:
		if v > math.MaxInt64 {
			return 0, false
		}
		return int64(v), true
	case uint64:
		if v > math.MaxInt64 {
			return 0, false
		}
		return int64(v), true
	case float64:
		if math.Trunc(v) != v {
			return 0, false
		}
		return int64(v), true
	case float32:
		if math.Trunc(float64(v)) != float64(v) {
			return 0, false
		}
		return int64(v), true
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err == nil {
			return parsed, true
		}
		floatValue, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil || math.Trunc(floatValue) != floatValue {
			return 0, false
		}
		return int64(floatValue), true
	case json.Number:
		parsed, err := v.Int64()
		if err == nil {
			return parsed, true
		}
		floatValue, err := v.Float64()
		if err != nil || math.Trunc(floatValue) != floatValue {
			return 0, false
		}
		return int64(floatValue), true
	default:
		return 0, false
	}
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
	case int32:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint64:
		return float64(v), true
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	case json.Number:
		parsed, err := v.Float64()
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
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
		parsed, ok := parseTimeString(v)
		if !ok {
			return time.Time{}, false
		}
		return parsed, true
	case int:
		return time.Unix(int64(v), 0), true
	case int64:
		return time.Unix(v, 0), true
	case float64:
		return time.Unix(int64(v), 0), true
	case json.Number:
		if parsed, err := v.Int64(); err == nil {
			return time.Unix(parsed, 0), true
		}
		floatValue, err := v.Float64()
		if err != nil {
			return time.Time{}, false
		}
		return time.Unix(int64(floatValue), 0), true
	default:
		return time.Time{}, false
	}
}

func parseTimeString(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02",
		"15:04:05",
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}
