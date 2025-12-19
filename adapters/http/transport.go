package exporthttp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/goliatone/go-export/adapters/exportapi"
)

type httpRequest struct {
	r *http.Request
}

func (req httpRequest) Context() context.Context {
	if req.r == nil {
		return context.Background()
	}
	return req.r.Context()
}

func (req httpRequest) Method() string {
	if req.r == nil {
		return ""
	}
	return req.r.Method
}

func (req httpRequest) Path() string {
	if req.r == nil || req.r.URL == nil {
		return ""
	}
	return req.r.URL.Path
}

func (req httpRequest) Header(name string) string {
	if req.r == nil {
		return ""
	}
	return req.r.Header.Get(name)
}

func (req httpRequest) Query(name string) string {
	if req.r == nil || req.r.URL == nil {
		return ""
	}
	return req.r.URL.Query().Get(name)
}

func (req httpRequest) Body() io.ReadCloser {
	if req.r == nil {
		return nil
	}
	return req.r.Body
}

type httpResponse struct {
	w   http.ResponseWriter
	req *http.Request
}

func (res httpResponse) SetHeader(name, value string) {
	if res.w == nil {
		return
	}
	res.w.Header().Set(name, value)
}

func (res httpResponse) DelHeader(name string) {
	if res.w == nil {
		return
	}
	res.w.Header().Del(name)
}

func (res httpResponse) WriteHeader(status int) {
	if res.w == nil {
		return
	}
	res.w.WriteHeader(status)
}

func (res httpResponse) Write(data []byte) (int, error) {
	if res.w == nil {
		return 0, nil
	}
	return res.w.Write(data)
}

func (res httpResponse) WriteJSON(status int, payload any) error {
	if res.w == nil {
		return nil
	}
	res.w.Header().Set("Content-Type", "application/json")
	res.w.WriteHeader(status)
	return json.NewEncoder(res.w).Encode(payload)
}

func (res httpResponse) Writer() (io.Writer, bool) {
	if res.w == nil {
		return nil, false
	}
	return res.w, true
}

func (res httpResponse) Redirect(location string, status int) error {
	if res.w == nil {
		return nil
	}
	if res.req != nil {
		http.Redirect(res.w, res.req, location, status)
		return nil
	}
	res.w.Header().Set("Location", location)
	res.w.WriteHeader(status)
	return nil
}

var _ exportapi.Response = httpResponse{}
