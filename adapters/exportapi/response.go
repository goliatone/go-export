package exportapi

import (
	"context"
	"io"
)

// DownloadPayload describes a file download response.
type DownloadPayload struct {
	ContentType    string
	Filename       string
	ExportID       string
	Size           int64
	MaxBufferBytes int64
	Reader         io.Reader
	Bytes          []byte
}

// StreamOptions configures download stream responses.
type StreamOptions struct {
	Filename       string
	ExportID       string
	ContentLength  int64
	MaxBufferBytes int64
}

// StreamOption applies stream options.
type StreamOption func(*StreamOptions)

// WithFilename sets the download filename.
func WithFilename(filename string) StreamOption {
	return func(opts *StreamOptions) {
		opts.Filename = filename
	}
}

// WithExportID sets the export id response header.
func WithExportID(exportID string) StreamOption {
	return func(opts *StreamOptions) {
		opts.ExportID = exportID
	}
}

// WithContentLength sets the content length header.
func WithContentLength(length int64) StreamOption {
	return func(opts *StreamOptions) {
		opts.ContentLength = length
	}
}

// WithMaxBufferBytes sets the maximum buffer size when streaming is unavailable.
func WithMaxBufferBytes(max int64) StreamOption {
	return func(opts *StreamOptions) {
		opts.MaxBufferBytes = max
	}
}

// ResolveStreamOptions applies stream options.
func ResolveStreamOptions(opts ...StreamOption) StreamOptions {
	resolved := StreamOptions{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&resolved)
	}
	return resolved
}

// DownloadResponder provides streaming download helpers.
type DownloadResponder interface {
	WriteDownload(ctx context.Context, payload DownloadPayload) error
	WriteStream(ctx context.Context, contentType string, r io.Reader, opts ...StreamOption) error
}

// Response provides a minimal response interface for transport adapters.
type Response interface {
	DownloadResponder
	SetHeader(name, value string)
	DelHeader(name string)
	WriteHeader(status int)
	Write(data []byte) (int, error)
	WriteJSON(status int, payload any) error
	Writer() (io.Writer, bool)
	Redirect(location string, status int) error
}

// AsyncResponse describes async export responses.
type AsyncResponse struct {
	ID          string `json:"id"`
	StatusURL   string `json:"status_url"`
	DownloadURL string `json:"download_url"`
}

// ErrorResponse describes JSON error responses.
type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

// ErrorBody contains error details.
type ErrorBody struct {
	Message string `json:"message"`
	Code    string `json:"code,omitempty"`
}
