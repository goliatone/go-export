package exporthttp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

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

func (req httpRequest) URL() *url.URL {
	if req.r == nil {
		return nil
	}
	return req.r.URL
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

func (res httpResponse) WriteDownload(ctx context.Context, payload exportapi.DownloadPayload) error {
	if payload.Reader == nil && payload.Bytes == nil {
		return nil
	}
	reader := payload.Reader
	size := payload.Size
	if reader == nil && payload.Bytes != nil {
		reader = bytes.NewReader(payload.Bytes)
		if size == 0 {
			size = int64(len(payload.Bytes))
		}
	}
	opts := []exportapi.StreamOption{
		exportapi.WithFilename(payload.Filename),
		exportapi.WithExportID(payload.ExportID),
		exportapi.WithContentLength(size),
		exportapi.WithMaxBufferBytes(payload.MaxBufferBytes),
	}
	return res.WriteStream(ctx, payload.ContentType, reader, opts...)
}

func (res httpResponse) WriteStream(ctx context.Context, contentType string, r io.Reader, opts ...exportapi.StreamOption) error {
	_ = ctx
	if res.w == nil {
		return nil
	}
	if r == nil {
		return nil
	}
	options := exportapi.ResolveStreamOptions(opts...)
	return writeHTTPStream(res.w, contentType, r, options)
}

func writeHTTPStream(w http.ResponseWriter, contentType string, r io.Reader, opts exportapi.StreamOptions) error {
	if w == nil {
		return nil
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	buf := make([]byte, 32*1024)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			applyDownloadHeaders(w, contentType, opts)
			w.WriteHeader(http.StatusOK)
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				return writeErr
			}
			if err == io.EOF {
				return nil
			}
			_, err = io.Copy(w, r)
			return err
		}
		if err != nil {
			if err == io.EOF {
				applyDownloadHeaders(w, contentType, opts)
				w.WriteHeader(http.StatusOK)
				return nil
			}
			return err
		}
	}
}

func applyDownloadHeaders(w http.ResponseWriter, contentType string, opts exportapi.StreamOptions) {
	if w == nil {
		return
	}
	w.Header().Set("Content-Type", contentType)
	if opts.Filename != "" {
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", opts.Filename))
	}
	if opts.ExportID != "" {
		w.Header().Set("X-Export-Id", opts.ExportID)
	}
	if opts.ContentLength > 0 {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", opts.ContentLength))
	}
}

var _ exportapi.Response = httpResponse{}
