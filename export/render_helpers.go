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

func stringify(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}
