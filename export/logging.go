package export

import (
	"github.com/goliatone/go-logger/glog"
)

// Logger is the canonical go-logger compatible logger contract.
type Logger = glog.Logger

// LoggerProvider is the go-logger compatible logger provider contract.
type LoggerProvider = glog.LoggerProvider

// FieldsLogger is the optional go-logger fields extension.
type FieldsLogger = glog.FieldsLogger

// NopLogger returns the canonical no-op logger.
func NopLogger() Logger {
	return glog.Nop()
}

// EnsureLogger guarantees a non-nil Logger.
func EnsureLogger(logger Logger) Logger {
	return glog.Ensure(logger)
}
