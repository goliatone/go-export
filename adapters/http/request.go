package exporthttp

import "github.com/goliatone/go-export/adapters/exportapi"

// RequestDecoder parses an HTTP request into an export request.
type RequestDecoder = exportapi.RequestDecoder

// QueryDecoder converts raw JSON query payloads into typed values.
type QueryDecoder = exportapi.QueryDecoder

// JSONRequestDecoder decodes JSON into export requests.
type JSONRequestDecoder = exportapi.JSONRequestDecoder
