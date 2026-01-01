package exportrouter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/goliatone/go-export/adapters/exportapi"
	"github.com/goliatone/go-export/export"
	"github.com/goliatone/go-router"
)

var _ exportapi.Response = routerResponse{}

type routerRequest struct {
	ctx router.Context
}

func (req routerRequest) Context() context.Context {
	if req.ctx == nil {
		return context.Background()
	}
	return req.ctx.Context()
}

func (req routerRequest) Method() string {
	if req.ctx == nil {
		return ""
	}
	return req.ctx.Method()
}

func (req routerRequest) Path() string {
	if req.ctx == nil {
		return ""
	}
	return req.ctx.Path()
}

func (req routerRequest) URL() *url.URL {
	if req.ctx == nil {
		return nil
	}
	if httpCtx, ok := router.AsHTTPContext(req.ctx); ok {
		if httpReq := httpCtx.Request(); httpReq != nil {
			return httpReq.URL
		}
	}
	raw := strings.TrimSpace(req.ctx.OriginalURL())
	if raw == "" {
		return nil
	}
	parsed, err := url.ParseRequestURI(raw)
	if err != nil {
		return nil
	}
	return parsed
}

func (req routerRequest) Header(name string) string {
	if req.ctx == nil {
		return ""
	}
	return req.ctx.Header(name)
}

func (req routerRequest) Query(name string) string {
	if req.ctx == nil {
		return ""
	}
	return req.ctx.Query(name)
}

func (req routerRequest) Body() io.ReadCloser {
	if req.ctx == nil {
		return nil
	}
	return io.NopCloser(bytes.NewReader(req.ctx.Body()))
}

type routerResponse struct {
	ctx router.Context
}

func (res routerResponse) SetHeader(name, value string) {
	if res.ctx == nil {
		return
	}
	res.ctx.SetHeader(name, value)
}

func (res routerResponse) DelHeader(name string) {
	if res.ctx == nil {
		return
	}
	res.ctx.SetHeader(name, "")
}

func (res routerResponse) WriteHeader(status int) {
	if res.ctx == nil {
		return
	}
	res.ctx.Status(status)
}

func (res routerResponse) Write(data []byte) (int, error) {
	if res.ctx == nil {
		return 0, nil
	}
	if err := res.ctx.Send(data); err != nil {
		return 0, err
	}
	return len(data), nil
}

func (res routerResponse) WriteJSON(status int, payload any) error {
	if res.ctx == nil {
		return nil
	}
	return res.ctx.JSON(status, payload)
}

func (res routerResponse) Writer() (io.Writer, bool) {
	if res.ctx == nil {
		return nil, false
	}
	httpCtx, ok := router.AsHTTPContext(res.ctx)
	if !ok || httpCtx.Response() == nil {
		return nil, false
	}
	return httpCtx.Response(), true
}

func (res routerResponse) Redirect(location string, status int) error {
	if res.ctx == nil {
		return nil
	}
	return res.ctx.Redirect(location, status)
}

func (res routerResponse) WriteDownload(ctx context.Context, payload exportapi.DownloadPayload) error {
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

func (res routerResponse) WriteStream(ctx context.Context, contentType string, r io.Reader, opts ...exportapi.StreamOption) error {
	_ = ctx
	if res.ctx == nil {
		return nil
	}
	if r == nil {
		return nil
	}
	options := exportapi.ResolveStreamOptions(opts...)
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	if httpCtx, ok := router.AsHTTPContext(res.ctx); ok && httpCtx.Response() != nil {
		return res.writeStreamToWriter(httpCtx.Response(), contentType, r, options)
	}
	data, err := readAllWithLimit(r, options.MaxBufferBytes)
	if err != nil {
		closeIfPossible(r)
		return err
	}
	res.applyDownloadHeaders(contentType, options)
	res.ctx.Status(http.StatusOK)
	return res.ctx.Send(data)
}

func (res routerResponse) writeStreamToWriter(w io.Writer, contentType string, r io.Reader, opts exportapi.StreamOptions) error {
	if w == nil {
		return nil
	}
	buf := make([]byte, 32*1024)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			res.applyDownloadHeaders(contentType, opts)
			res.ctx.Status(http.StatusOK)
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
				res.applyDownloadHeaders(contentType, opts)
				res.ctx.Status(http.StatusOK)
				return nil
			}
			return err
		}
	}
}

func (res routerResponse) applyDownloadHeaders(contentType string, opts exportapi.StreamOptions) {
	if res.ctx == nil {
		return
	}
	res.ctx.SetHeader("Content-Type", contentType)
	if opts.Filename != "" {
		res.ctx.SetHeader("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", opts.Filename))
	}
	if opts.ExportID != "" {
		res.ctx.SetHeader("X-Export-Id", opts.ExportID)
	}
	if opts.ContentLength > 0 {
		res.ctx.SetHeader("Content-Length", fmt.Sprintf("%d", opts.ContentLength))
	}
}

func readAllWithLimit(r io.Reader, maxSize int64) ([]byte, error) {
	if r == nil {
		return nil, nil
	}
	if maxSize <= 0 {
		maxSize = exportapi.DefaultMaxBufferBytes
	}
	limited := io.LimitReader(r, maxSize+1)
	buf := &bytes.Buffer{}
	if _, err := io.Copy(buf, limited); err != nil {
		return nil, err
	}
	if int64(buf.Len()) > maxSize {
		return nil, export.NewError(export.KindInternal, "buffer limit exceeded", nil)
	}
	return buf.Bytes(), nil
}

func closeIfPossible(r io.Reader) {
	if r == nil {
		return
	}
	if closer, ok := r.(io.Closer); ok {
		_ = closer.Close()
	}
}
