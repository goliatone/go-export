package exportapi

import "io"

// Response provides a minimal response interface for transport adapters.
type Response interface {
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
