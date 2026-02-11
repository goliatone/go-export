package loggingcompat

import (
	"context"
	"testing"

	export "github.com/goliatone/go-export/export"
)

type testLogger struct {
	count int
}

func (l *testLogger) Trace(msg string, args ...any) {
	l.count++
}

func (l *testLogger) Debug(msg string, args ...any) {
	l.count++
}

func (l *testLogger) Info(msg string, args ...any) {
	l.count++
}

func (l *testLogger) Warn(msg string, args ...any) {
	l.count++
}

func (l *testLogger) Error(msg string, args ...any) {
	l.count++
}

func (l *testLogger) Fatal(msg string, args ...any) {
	l.count++
}

func (l *testLogger) WithContext(ctx context.Context) export.Logger {
	_ = ctx
	return l
}

func TestNopLoggerIsSafe(t *testing.T) {
	logger := export.NopLogger()
	logger.Trace("trace")
	logger.Debug("debug")
	logger.Info("info")
	logger.Warn("warn")
	logger.Error("error")
	logger.Fatal("fatal")
	_ = logger.WithContext(context.Background())
}

func TestEnsureLogger(t *testing.T) {
	fallback := export.EnsureLogger(nil)
	if fallback == nil {
		t.Fatalf("expected non-nil fallback logger")
	}

	base := &testLogger{}
	resolved := export.EnsureLogger(base)
	resolved.Info("ok")
	if base.count == 0 {
		t.Fatalf("expected wrapped logger to receive call")
	}
}

var _ export.Logger = (*testLogger)(nil)
