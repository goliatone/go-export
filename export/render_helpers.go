package export

import (
	"fmt"
	"io"
)

type countingWriter struct {
	w     io.Writer
	count int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.count += int64(n)
	return n, err
}

type limitedWriter struct {
	w     io.Writer
	count int64
	limit int64
}

func newLimitedWriter(w io.Writer, limit int64) *limitedWriter {
	return &limitedWriter{w: w, limit: limit}
}

func (lw *limitedWriter) Write(p []byte) (int, error) {
	if lw.limit > 0 && lw.count+int64(len(p)) > lw.limit {
		return 0, NewError(KindValidation, "max bytes exceeded", nil)
	}
	n, err := lw.w.Write(p)
	lw.count += int64(n)
	if lw.limit > 0 && lw.count > lw.limit {
		return n, NewError(KindValidation, "max bytes exceeded", nil)
	}
	return n, err
}

func stringify(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}
