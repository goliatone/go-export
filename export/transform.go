package export

import (
	"context"
	"fmt"
	"io"
)

// TransformerConfig identifies a configured transformer in a pipeline.
type TransformerConfig struct {
	Key    string         `json:"key"`
	Params map[string]any `json:"params,omitempty"`
}

// RowMapFunc maps a row to a new row.
type RowMapFunc func(ctx context.Context, row Row) (Row, error)

// RowFilterFunc decides whether a row should be kept.
type RowFilterFunc func(ctx context.Context, row Row) (bool, error)

// RowAugmentFunc returns additional values to append to a row.
type RowAugmentFunc func(ctx context.Context, row Row) ([]any, error)

// MapTransformer applies a mapping function to each row.
type MapTransformer struct {
	MapFunc RowMapFunc
}

// NewMapTransformer creates a MapTransformer.
func NewMapTransformer(fn RowMapFunc) MapTransformer {
	return MapTransformer{MapFunc: fn}
}

// Wrap implements RowTransformer.
func (t MapTransformer) Wrap(ctx context.Context, in RowIterator, schema Schema) (RowIterator, Schema, error) {
	if t.MapFunc == nil {
		return nil, Schema{}, NewError(KindValidation, "map transformer function is required", nil)
	}
	return &mapIterator{base: in, mapFn: t.MapFunc, schemaLen: len(schema.Columns)}, schema, nil
}

// FilterTransformer drops rows that do not pass the filter.
type FilterTransformer struct {
	FilterFunc RowFilterFunc
}

// NewFilterTransformer creates a FilterTransformer.
func NewFilterTransformer(fn RowFilterFunc) FilterTransformer {
	return FilterTransformer{FilterFunc: fn}
}

// Wrap implements RowTransformer.
func (t FilterTransformer) Wrap(ctx context.Context, in RowIterator, schema Schema) (RowIterator, Schema, error) {
	if t.FilterFunc == nil {
		return nil, Schema{}, NewError(KindValidation, "filter transformer function is required", nil)
	}
	return &filterIterator{base: in, filterFn: t.FilterFunc}, schema, nil
}

// AugmentTransformer appends derived columns to each row.
type AugmentTransformer struct {
	Columns     []Column
	AugmentFunc RowAugmentFunc
}

// NewAugmentTransformer creates an AugmentTransformer.
func NewAugmentTransformer(columns []Column, fn RowAugmentFunc) AugmentTransformer {
	return AugmentTransformer{Columns: columns, AugmentFunc: fn}
}

// Wrap implements RowTransformer.
func (t AugmentTransformer) Wrap(ctx context.Context, in RowIterator, schema Schema) (RowIterator, Schema, error) {
	if t.AugmentFunc == nil {
		return nil, Schema{}, NewError(KindValidation, "augment transformer function is required", nil)
	}
	if len(t.Columns) == 0 {
		return nil, Schema{}, NewError(KindValidation, "augment transformer columns are required", nil)
	}
	nextSchema := Schema{Columns: append(append([]Column{}, schema.Columns...), t.Columns...)}
	return &augmentIterator{
		base:       in,
		augmentFn:  t.AugmentFunc,
		baseLen:    len(schema.Columns),
		augmentLen: len(t.Columns),
	}, nextSchema, nil
}

func applyTransformers(ctx context.Context, rows RowIterator, schema Schema, transformers []RowTransformer) (RowIterator, Schema, error) {
	if len(transformers) == 0 {
		return rows, schema, nil
	}
	if err := validateSchema(schema); err != nil {
		return nil, Schema{}, err
	}

	currentRows := rows
	currentSchema := schema
	for idx, transformer := range transformers {
		if transformer == nil {
			return nil, Schema{}, NewError(KindValidation, fmt.Sprintf("transformer %d is nil", idx), nil)
		}
		wrapped, nextSchema, err := transformer.Wrap(ctx, currentRows, currentSchema)
		if err != nil {
			return nil, Schema{}, err
		}
		if wrapped == nil {
			return nil, Schema{}, NewError(KindValidation, fmt.Sprintf("transformer %d returned nil iterator", idx), nil)
		}
		if err := validateSchema(nextSchema); err != nil {
			return nil, Schema{}, err
		}
		currentRows = wrapped
		currentSchema = nextSchema
	}

	return newSchemaGuardIterator(currentRows, currentSchema), currentSchema, nil
}

type mapIterator struct {
	base      RowIterator
	mapFn     RowMapFunc
	schemaLen int
}

func (it *mapIterator) Next(ctx context.Context) (Row, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	row, err := it.base.Next(ctx)
	if err != nil {
		return nil, err
	}
	mapped, err := it.mapFn(ctx, row)
	if err != nil {
		return nil, err
	}
	if len(mapped) != it.schemaLen {
		return nil, NewError(KindValidation, "row length does not match schema", nil)
	}
	return mapped, nil
}

func (it *mapIterator) Close() error {
	return it.base.Close()
}

type filterIterator struct {
	base     RowIterator
	filterFn RowFilterFunc
}

func (it *filterIterator) Next(ctx context.Context) (Row, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		row, err := it.base.Next(ctx)
		if err != nil {
			return nil, err
		}
		keep, err := it.filterFn(ctx, row)
		if err != nil {
			return nil, err
		}
		if keep {
			return row, nil
		}
	}
}

func (it *filterIterator) Close() error {
	return it.base.Close()
}

type augmentIterator struct {
	base       RowIterator
	augmentFn  RowAugmentFunc
	baseLen    int
	augmentLen int
}

func (it *augmentIterator) Next(ctx context.Context) (Row, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	row, err := it.base.Next(ctx)
	if err != nil {
		return nil, err
	}
	if it.baseLen > 0 && len(row) != it.baseLen {
		return nil, NewError(KindValidation, "row length does not match schema", nil)
	}
	extra, err := it.augmentFn(ctx, row)
	if err != nil {
		return nil, err
	}
	if len(extra) != it.augmentLen {
		return nil, NewError(KindValidation, "row length does not match schema", nil)
	}
	combined := make(Row, 0, len(row)+len(extra))
	combined = append(combined, row...)
	combined = append(combined, extra...)
	return combined, nil
}

func (it *augmentIterator) Close() error {
	return it.base.Close()
}

type bufferedTransformer struct {
	transformer BufferedTransformer
	maxRows     int
	maxBytes    int64
}

func newBufferedTransformer(transformer BufferedTransformer, policy ExportPolicy) RowTransformer {
	return bufferedTransformer{
		transformer: transformer,
		maxRows:     policy.MaxRows,
		maxBytes:    policy.MaxBytes,
	}
}

func (t bufferedTransformer) Wrap(ctx context.Context, in RowIterator, schema Schema) (RowIterator, Schema, error) {
	if t.transformer == nil {
		return nil, Schema{}, NewError(KindValidation, "buffered transformer is required", nil)
	}
	limited := newLimitedIterator(in, t.maxRows, t.maxBytes)
	rows, nextSchema, err := t.transformer.Process(ctx, limited, schema)
	_ = limited.Close()
	if err != nil {
		return nil, Schema{}, err
	}
	return &sliceIterator{rows: rows}, nextSchema, nil
}

type limitedIterator struct {
	base     RowIterator
	maxRows  int
	maxBytes int64
	rows     int
	bytes    int64
}

func newLimitedIterator(base RowIterator, maxRows int, maxBytes int64) *limitedIterator {
	return &limitedIterator{
		base:     base,
		maxRows:  maxRows,
		maxBytes: maxBytes,
	}
}

func (it *limitedIterator) Next(ctx context.Context) (Row, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	row, err := it.base.Next(ctx)
	if err != nil {
		return nil, err
	}
	it.rows++
	if it.maxRows > 0 && it.rows > it.maxRows {
		return nil, NewError(KindValidation, "buffered transform max rows exceeded", nil)
	}
	if it.maxBytes > 0 {
		it.bytes += estimateRowBytes(row)
		if it.bytes > it.maxBytes {
			return nil, NewError(KindValidation, "buffered transform max bytes exceeded", nil)
		}
	}
	return row, nil
}

func (it *limitedIterator) Close() error {
	return it.base.Close()
}

type sliceIterator struct {
	rows  []Row
	index int
}

func (it *sliceIterator) Next(ctx context.Context) (Row, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if it.index >= len(it.rows) {
		return nil, io.EOF
	}
	row := it.rows[it.index]
	it.index++
	return row, nil
}

func (it *sliceIterator) Close() error {
	return nil
}

type schemaGuardIterator struct {
	base      RowIterator
	schemaLen int
}

func newSchemaGuardIterator(base RowIterator, schema Schema) RowIterator {
	return &schemaGuardIterator{base: base, schemaLen: len(schema.Columns)}
}

func (it *schemaGuardIterator) Next(ctx context.Context) (Row, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	row, err := it.base.Next(ctx)
	if err != nil {
		return nil, err
	}
	if len(row) != it.schemaLen {
		return nil, NewError(KindValidation, "row length does not match schema", nil)
	}
	return row, nil
}

func (it *schemaGuardIterator) Close() error {
	return it.base.Close()
}

func estimateRowBytes(row Row) int64 {
	if len(row) == 0 {
		return 0
	}
	var total int64
	for _, value := range row {
		switch v := value.(type) {
		case nil:
			continue
		case string:
			total += int64(len(v))
		case []byte:
			total += int64(len(v))
		case fmt.Stringer:
			total += int64(len(v.String()))
		default:
			total += int64(len(fmt.Sprint(v)))
		}
	}
	return total
}
