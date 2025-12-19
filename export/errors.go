package export

import (
	"context"
	"errors"

	errorslib "github.com/goliatone/go-errors"
)

// ErrorKind defines export error kinds.
type ErrorKind string

const (
	KindValidation ErrorKind = "validation"
	KindAuthz      ErrorKind = "authz"
	KindNotFound   ErrorKind = "not_found"
	KindTimeout    ErrorKind = "timeout"
	KindCanceled   ErrorKind = "canceled"
	KindInternal   ErrorKind = "internal"
	KindNotImpl    ErrorKind = "not_implemented"
)

// ExportError wraps errors with a kind.
type ExportError struct {
	Kind ErrorKind
	Msg  string
	Err  error
}

func (e *ExportError) Error() string {
	if e.Err == nil {
		return e.Msg
	}
	return e.Msg + ": " + e.Err.Error()
}

func (e *ExportError) Unwrap() error {
	return e.Err
}

// NewError creates a new export error.
func NewError(kind ErrorKind, msg string, err error) *ExportError {
	return &ExportError{Kind: kind, Msg: msg, Err: err}
}

// AsGoError maps an error into a go-errors error.
func AsGoError(err error) *errorslib.Error {
	if err == nil {
		return nil
	}

	var ge *errorslib.Error
	if errors.As(err, &ge) {
		return ge
	}

	kind := KindInternal
	msg := err.Error()

	var exportErr *ExportError
	if errors.As(err, &exportErr) {
		kind = exportErr.Kind
		if exportErr.Msg != "" {
			msg = exportErr.Msg
		}
	}

	if errors.Is(err, context.DeadlineExceeded) {
		kind = KindTimeout
	}
	if errors.Is(err, context.Canceled) {
		kind = KindCanceled
	}

	switch kind {
	case KindValidation:
		return errorslib.New(msg, errorslib.CategoryValidation).WithTextCode("validation")
	case KindAuthz:
		return errorslib.New(msg, errorslib.CategoryAuthz).WithTextCode("authz")
	case KindNotFound:
		return errorslib.New(msg, errorslib.CategoryNotFound).WithTextCode("not_found")
	case KindTimeout:
		return errorslib.New(msg, errorslib.CategoryOperation).WithTextCode("timeout")
	case KindCanceled:
		return errorslib.New(msg, errorslib.CategoryOperation).WithTextCode("canceled")
	case KindNotImpl:
		return errorslib.New(msg, errorslib.CategoryOperation).WithTextCode("not_implemented")
	default:
		return errorslib.New(msg, errorslib.CategoryInternal).WithTextCode("internal")
	}
}

// KindFromError maps an error to its export error kind.
func KindFromError(err error) ErrorKind {
	if err == nil {
		return ""
	}

	var exportErr *ExportError
	if errors.As(err, &exportErr) {
		return exportErr.Kind
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return KindTimeout
	}
	if errors.Is(err, context.Canceled) {
		return KindCanceled
	}

	return KindInternal
}
