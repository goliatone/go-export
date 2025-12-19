package export

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/xuri/excelize/v2"
)

const (
	excelMaxRows      = 1048576
	defaultSheetName  = "Sheet1"
	defaultDateFormat = "yyyy-mm-dd"
	defaultDateTime   = "yyyy-mm-dd hh:mm:ss"
	defaultTimeFormat = "hh:mm:ss"
	defaultFloatFmt   = "0.00"
)

// XLSXRenderer renders XLSX output.
type XLSXRenderer struct{}

// Render streams rows into an XLSX workbook.
func (r XLSXRenderer) Render(ctx context.Context, schema Schema, rows RowIterator, w io.Writer, opts RenderOptions) (RenderStats, error) {
	formatter, err := newFormatContext(opts.Format)
	if err != nil {
		return RenderStats{}, err
	}

	file := excelize.NewFile()
	defer func() {
		_ = file.Close()
	}()

	sheetName := opts.XLSX.SheetName
	if sheetName == "" {
		sheetName = defaultSheetName
	}
	defaultSheet := file.GetSheetName(0)
	if defaultSheet != sheetName {
		file.SetSheetName(defaultSheet, sheetName)
	}

	stream, err := file.NewStreamWriter(sheetName)
	if err != nil {
		return RenderStats{}, err
	}

	styles, err := buildXLSXStyles(file)
	if err != nil {
		return RenderStats{}, err
	}
	columnStyles, err := styles.forColumns(schema.Columns)
	if err != nil {
		return RenderStats{}, err
	}

	rowIndex := 1
	if opts.XLSX.IncludeHeaders {
		headers := make([]interface{}, len(schema.Columns))
		for i, col := range schema.Columns {
			label := col.Label
			if label == "" {
				label = col.Name
			}
			headers[i] = excelize.Cell{StyleID: styles.headerID, Value: label}
		}
		if err := stream.SetRow(fmt.Sprintf("A%d", rowIndex), headers); err != nil {
			return RenderStats{}, err
		}
		rowIndex++
	}

	maxRows := opts.XLSX.MaxRows
	if maxRows <= 0 || maxRows > excelMaxRows {
		maxRows = excelMaxRows
	}
	if opts.XLSX.IncludeHeaders && maxRows > excelMaxRows-1 {
		maxRows = excelMaxRows - 1
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

		stats.Rows++
		if maxRows > 0 && stats.Rows > int64(maxRows) {
			return stats, NewError(KindValidation, "max rows exceeded", nil)
		}
		if rowIndex > excelMaxRows {
			return stats, NewError(KindValidation, "xlsx row limit exceeded", nil)
		}

		cells := make([]interface{}, len(row))
		for i, value := range row {
			cell, err := buildXLSXCell(schema.Columns[i], value, formatter, columnStyles[i])
			if err != nil {
				return stats, err
			}
			cells[i] = cell
		}

		if err := stream.SetRow(fmt.Sprintf("A%d", rowIndex), cells); err != nil {
			return stats, err
		}
		rowIndex++
	}

	if err := stream.Flush(); err != nil {
		return stats, err
	}

	lw := newLimitedWriter(w, opts.XLSX.MaxBytes)
	if _, err := file.WriteTo(lw); err != nil {
		return stats, err
	}
	stats.Bytes = lw.count
	return stats, nil
}

type xlsxStyles struct {
	headerID  int
	dateID    int
	dateTime  int
	timeID    int
	floatID   int
	customIDs map[string]int
	file      *excelize.File
}

func buildXLSXStyles(file *excelize.File) (*xlsxStyles, error) {
	headerID, err := file.NewStyle(&excelize.Style{Font: &excelize.Font{Bold: true}})
	if err != nil {
		return nil, err
	}

	dateID, err := newCustomStyle(file, defaultDateFormat)
	if err != nil {
		return nil, err
	}
	dateTimeID, err := newCustomStyle(file, defaultDateTime)
	if err != nil {
		return nil, err
	}
	timeID, err := newCustomStyle(file, defaultTimeFormat)
	if err != nil {
		return nil, err
	}
	floatID, err := newCustomStyle(file, defaultFloatFmt)
	if err != nil {
		return nil, err
	}

	return &xlsxStyles{
		headerID:  headerID,
		dateID:    dateID,
		dateTime:  dateTimeID,
		timeID:    timeID,
		floatID:   floatID,
		customIDs: make(map[string]int),
		file:      file,
	}, nil
}

func newCustomStyle(file *excelize.File, format string) (int, error) {
	if format == "" {
		return 0, nil
	}
	return file.NewStyle(&excelize.Style{CustomNumFmt: &format})
}

func (s *xlsxStyles) forColumns(columns []Column) ([]int, error) {
	styles := make([]int, len(columns))
	for i, col := range columns {
		styleID := 0
		if col.Format.Excel != "" {
			custom, err := s.customStyle(col.Format.Excel)
			if err != nil {
				return nil, err
			}
			styleID = custom
		} else {
			switch normalizeColumnType(col.Type) {
			case "date":
				styleID = s.dateID
			case "datetime", "timestamp":
				styleID = s.dateTime
			case "time":
				styleID = s.timeID
			case "float", "float64", "decimal", "number":
				styleID = s.floatID
			}
		}
		styles[i] = styleID
	}
	return styles, nil
}

func (s *xlsxStyles) customStyle(format string) (int, error) {
	if format == "" {
		return 0, nil
	}
	if styleID, ok := s.customIDs[format]; ok {
		return styleID, nil
	}
	styleID, err := newCustomStyle(s.file, format)
	if err != nil {
		return 0, err
	}
	s.customIDs[format] = styleID
	return styleID, nil
}

func buildXLSXCell(col Column, value any, formatter formatContext, styleID int) (excelize.Cell, error) {
	if value == nil {
		return excelize.Cell{Value: ""}, nil
	}

	colType := normalizeColumnType(col.Type)
	switch colType {
	case "string":
		return excelize.Cell{Value: stringify(value), StyleID: styleID}, nil
	case "bool", "boolean":
		boolValue, ok := coerceBool(value)
		if !ok {
			return excelize.Cell{}, NewError(KindValidation, fmt.Sprintf("invalid bool for column %q", col.Name), nil)
		}
		return excelize.Cell{Value: boolValue, StyleID: styleID}, nil
	case "int", "integer", "int64":
		intValue, ok := coerceInt(value)
		if !ok {
			return excelize.Cell{}, NewError(KindValidation, fmt.Sprintf("invalid int for column %q", col.Name), nil)
		}
		return excelize.Cell{Value: intValue, StyleID: styleID}, nil
	case "float", "float64", "decimal", "number":
		floatValue, ok := coerceFloat(value)
		if !ok {
			return excelize.Cell{}, NewError(KindValidation, fmt.Sprintf("invalid number for column %q", col.Name), nil)
		}
		return excelize.Cell{Value: floatValue, StyleID: styleID}, nil
	case "date", "datetime", "time", "timestamp":
		timeValue, ok := coerceTime(value)
		if !ok {
			return excelize.Cell{}, NewError(KindValidation, fmt.Sprintf("invalid time for column %q", col.Name), nil)
		}
		timeValue = formatter.applyTimezone(timeValue)
		return excelize.Cell{Value: timeValue, StyleID: styleID}, nil
	}

	switch v := value.(type) {
	case time.Time:
		return excelize.Cell{Value: formatter.applyTimezone(v), StyleID: styleID}, nil
	case bool, int, int64, float64, float32, string:
		return excelize.Cell{Value: v, StyleID: styleID}, nil
	default:
		return excelize.Cell{Value: stringify(value), StyleID: styleID}, nil
	}
}
