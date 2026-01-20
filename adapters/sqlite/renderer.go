package exportsqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/goliatone/go-export/export"
	_ "modernc.org/sqlite"
)

const defaultTableName = "data"

// Renderer writes rows into a SQLite database file (disabled by default).
type Renderer struct {
	Enabled   bool
	TableName string
}

// Render buffers rows into a temp SQLite database and streams it to w.
func (r Renderer) Render(ctx context.Context, schema export.Schema, rows export.RowIterator, w io.Writer, opts export.RenderOptions) (export.RenderStats, error) {
	if !r.Enabled {
		return export.RenderStats{}, export.NewError(export.KindNotImpl, "sqlite renderer is disabled", nil)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if len(schema.Columns) == 0 {
		return export.RenderStats{}, export.NewError(export.KindValidation, "schema has no columns", nil)
	}

	formatter, err := newFormatContext(opts.Format)
	if err != nil {
		return export.RenderStats{}, err
	}

	tableName := strings.TrimSpace(opts.SQLite.TableName)
	if tableName == "" {
		tableName = strings.TrimSpace(r.TableName)
	}
	tableName = sanitizeIdentifier(tableName, defaultTableName)
	spec, err := buildTableSpec(schema, tableName)
	if err != nil {
		return export.RenderStats{}, err
	}

	tempFile, err := os.CreateTemp("", "go-export-*.sqlite")
	if err != nil {
		return export.RenderStats{}, export.NewError(export.KindInternal, "sqlite temp file create failed", err)
	}
	path := tempFile.Name()
	if err := tempFile.Close(); err != nil {
		_ = os.Remove(path)
		return export.RenderStats{}, export.NewError(export.KindInternal, "sqlite temp file close failed", err)
	}
	defer func() {
		_ = os.Remove(path)
	}()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return export.RenderStats{}, export.NewError(export.KindInternal, "sqlite open failed", err)
	}

	stats, err := writeSQLiteRows(ctx, db, spec, rows, formatter)
	if err != nil {
		_ = db.Close()
		return stats, err
	}
	if err := db.Close(); err != nil {
		return stats, export.NewError(export.KindInternal, "sqlite close failed", err)
	}

	file, err := os.Open(path)
	if err != nil {
		return stats, export.NewError(export.KindInternal, "sqlite temp file open failed", err)
	}
	defer func() {
		_ = file.Close()
	}()

	cw := &countingWriter{w: w}
	if _, err := io.Copy(cw, file); err != nil {
		return export.RenderStats{Rows: stats.Rows, Bytes: cw.count}, err
	}
	stats.Bytes = cw.count
	return stats, nil
}

type tableSpec struct {
	tableName string
	columns   []columnSpec
	createSQL string
	insertSQL string
}

type columnSpec struct {
	name    string
	sqlType string
	column  export.Column
}

func buildTableSpec(schema export.Schema, tableName string) (tableSpec, error) {
	if len(schema.Columns) == 0 {
		return tableSpec{}, export.NewError(export.KindValidation, "schema has no columns", nil)
	}

	seen := make(map[string]struct{}, len(schema.Columns))
	columns := make([]columnSpec, len(schema.Columns))
	columnDefs := make([]string, len(schema.Columns))
	columnNames := make([]string, len(schema.Columns))

	for i, col := range schema.Columns {
		name := strings.TrimSpace(col.Label)
		if name == "" {
			name = col.Name
		}
		if name == "" {
			return tableSpec{}, export.NewError(export.KindValidation, "column name is required", nil)
		}
		key := strings.ToLower(name)
		if _, ok := seen[key]; ok {
			return tableSpec{}, export.NewError(export.KindValidation, fmt.Sprintf("duplicate column label %q", name), nil)
		}
		seen[key] = struct{}{}

		sqlType := sqliteColumnType(col.Type)
		columns[i] = columnSpec{name: name, sqlType: sqlType, column: col}
		columnDefs[i] = fmt.Sprintf("%s %s", quoteIdentifier(name), sqlType)
		columnNames[i] = quoteIdentifier(name)
	}

	createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", quoteIdentifier(tableName), strings.Join(columnDefs, ", "))
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", quoteIdentifier(tableName), strings.Join(columnNames, ", "), strings.Join(placeholders(len(columns)), ", "))

	return tableSpec{
		tableName: tableName,
		columns:   columns,
		createSQL: createSQL,
		insertSQL: insertSQL,
	}, nil
}

func writeSQLiteRows(ctx context.Context, db *sql.DB, spec tableSpec, rows export.RowIterator, formatter formatContext) (export.RenderStats, error) {
	stats := export.RenderStats{}
	if ctx == nil {
		ctx = context.Background()
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return stats, export.NewError(export.KindInternal, "sqlite begin transaction failed", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if _, err := tx.ExecContext(ctx, spec.createSQL); err != nil {
		return stats, export.NewError(export.KindInternal, "sqlite create table failed", err)
	}

	stmt, err := tx.PrepareContext(ctx, spec.insertSQL)
	if err != nil {
		return stats, export.NewError(export.KindInternal, "sqlite prepare insert failed", err)
	}

	for {
		if err := ctx.Err(); err != nil {
			_ = stmt.Close()
			return stats, err
		}

		row, err := rows.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			_ = stmt.Close()
			return stats, err
		}
		if len(row) != len(spec.columns) {
			_ = stmt.Close()
			return stats, export.NewError(export.KindValidation, "row length does not match schema", nil)
		}

		values := make([]any, len(row))
		for i, value := range row {
			formatted, err := formatSQLiteValue(spec.columns[i].column, value, formatter)
			if err != nil {
				_ = stmt.Close()
				return stats, err
			}
			values[i] = formatted
		}

		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			_ = stmt.Close()
			return stats, export.NewError(export.KindInternal, "sqlite insert failed", err)
		}
		stats.Rows++
	}

	if err := stmt.Close(); err != nil {
		return stats, export.NewError(export.KindInternal, "sqlite close statement failed", err)
	}
	if err := tx.Commit(); err != nil {
		return stats, export.NewError(export.KindInternal, "sqlite commit failed", err)
	}
	return stats, nil
}

func sqliteColumnType(colType string) string {
	switch normalizeColumnType(colType) {
	case "bool", "int":
		return "INTEGER"
	case "float":
		return "REAL"
	case "date", "datetime", "time", "string":
		return "TEXT"
	default:
		return "TEXT"
	}
}

func formatSQLiteValue(col export.Column, value any, formatter formatContext) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch normalizeColumnType(col.Type) {
	case "date", "datetime", "time":
		timeValue, ok := coerceTime(value)
		if !ok {
			return nil, export.NewError(export.KindValidation, fmt.Sprintf("invalid time for column %q", col.Name), nil)
		}
		timeValue = formatter.applyTimezone(timeValue)
		layout := strings.TrimSpace(col.Format.Layout)
		if layout == "" {
			layout = defaultLayoutForType(normalizeColumnType(col.Type))
		}
		return timeValue.Format(layout), nil
	case "bool":
		boolValue, ok := coerceBool(value)
		if !ok {
			return nil, export.NewError(export.KindValidation, fmt.Sprintf("invalid bool for column %q", col.Name), nil)
		}
		if boolValue {
			return int64(1), nil
		}
		return int64(0), nil
	case "int":
		intValue, ok := coerceInt(value)
		if !ok {
			return nil, export.NewError(export.KindValidation, fmt.Sprintf("invalid int for column %q", col.Name), nil)
		}
		return intValue, nil
	case "float":
		floatValue, ok := coerceFloat(value)
		if !ok {
			return nil, export.NewError(export.KindValidation, fmt.Sprintf("invalid number for column %q", col.Name), nil)
		}
		return floatValue, nil
	default:
		return stringify(value), nil
	}
}

func placeholders(count int) []string {
	if count <= 0 {
		return nil
	}
	out := make([]string, count)
	for i := 0; i < count; i++ {
		out[i] = "?"
	}
	return out
}

func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func sanitizeIdentifier(name, fallback string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return fallback
	}
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	sanitized := strings.Trim(b.String(), "_")
	if sanitized == "" {
		sanitized = fallback
	}
	if sanitized != "" && sanitized[0] >= '0' && sanitized[0] <= '9' {
		sanitized = "t_" + sanitized
	}
	return sanitized
}

type countingWriter struct {
	w     io.Writer
	count int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.count += int64(n)
	return n, err
}

type formatContext struct {
	locale   string
	location *time.Location
}

func newFormatContext(opts export.FormatOptions) (formatContext, error) {
	ctx := formatContext{locale: strings.TrimSpace(opts.Locale)}
	if tz := strings.TrimSpace(opts.Timezone); tz != "" {
		loc, err := time.LoadLocation(tz)
		if err != nil {
			return formatContext{}, export.NewError(export.KindValidation, "invalid timezone", err)
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
		return int64(v), true
	case uint64:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint8:
		return int64(v), true
	case float64:
		return int64(v), true
	case float32:
		return int64(v), true
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	case json.Number:
		parsed, err := v.Int64()
		if err != nil {
			return 0, false
		}
		return parsed, true
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

func stringify(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}
