package export

import (
	"context"
	"encoding/json"
	"io"
)

// JSONRenderer renders JSON output.
type JSONRenderer struct{}

// Render streams rows as JSON array or NDJSON.
func (r JSONRenderer) Render(ctx context.Context, schema Schema, rows RowIterator, w io.Writer, opts RenderOptions) (RenderStats, error) {
	cw := &countingWriter{w: w}
	stats := RenderStats{}

	mode := opts.JSON.Mode
	if mode == "" {
		mode = JSONModeArray
	}

	if mode == JSONModeLines {
		encoder := json.NewEncoder(cw)
		for {
			if err := ctx.Err(); err != nil {
				return stats, err
			}
			row, err := rows.Next(ctx)
			if err != nil {
				if err == io.EOF {
					break
				}
				return stats, err
			}
			if len(row) != len(schema.Columns) {
				return stats, NewError(KindValidation, "row length does not match schema", nil)
			}

			obj := make(map[string]any, len(schema.Columns))
			for i, col := range schema.Columns {
				obj[col.Name] = row[i]
			}
			if err := encoder.Encode(obj); err != nil {
				return stats, err
			}
			stats.Rows++
		}

		stats.Bytes = cw.count
		return stats, nil
	}

	if _, err := cw.Write([]byte("[")); err != nil {
		return stats, err
	}

	first := true
	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		row, err := rows.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return stats, err
		}
		if len(row) != len(schema.Columns) {
			return stats, NewError(KindValidation, "row length does not match schema", nil)
		}

		obj := make(map[string]any, len(schema.Columns))
		for i, col := range schema.Columns {
			obj[col.Name] = row[i]
		}
		payload, err := json.Marshal(obj)
		if err != nil {
			return stats, err
		}
		if !first {
			if _, err := cw.Write([]byte(",")); err != nil {
				return stats, err
			}
		}
		first = false
		if _, err := cw.Write(payload); err != nil {
			return stats, err
		}
		stats.Rows++
	}

	if _, err := cw.Write([]byte("]")); err != nil {
		return stats, err
	}

	stats.Bytes = cw.count
	return stats, nil
}
