package export

import (
	"context"
	"encoding/csv"
	"io"
)

// CSVRenderer renders CSV output.
type CSVRenderer struct{}

// Render streams rows as CSV.
func (r CSVRenderer) Render(ctx context.Context, schema Schema, rows RowIterator, w io.Writer, opts RenderOptions) (RenderStats, error) {
	cw := &countingWriter{w: w}
	writer := csv.NewWriter(cw)
	if opts.CSV.Delimiter != 0 {
		writer.Comma = opts.CSV.Delimiter
	}

	formatter, err := newFormatContext(opts.Format)
	if err != nil {
		return RenderStats{}, err
	}

	if opts.CSV.IncludeHeaders {
		headers := make([]string, 0, len(schema.Columns))
		for _, col := range schema.Columns {
			label := col.Label
			if label == "" {
				label = col.Name
			}
			headers = append(headers, label)
		}
		if err := writer.Write(headers); err != nil {
			return RenderStats{}, err
		}
	}

	stats := RenderStats{}
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

		record := make([]string, len(row))
		for i, value := range row {
			formatted, err := formatter.formatTextValue(schema.Columns[i], value)
			if err != nil {
				return stats, err
			}
			record[i] = formatted
		}
		if err := writer.Write(record); err != nil {
			return stats, err
		}
		stats.Rows++
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return stats, err
	}

	stats.Bytes = cw.count
	return stats, nil
}
