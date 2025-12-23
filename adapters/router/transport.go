package exportrouter

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"strings"

	"github.com/goliatone/go-export/adapters/exportapi"
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
