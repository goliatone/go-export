package exporthttp

import (
	"net/http"

	"github.com/goliatone/go-export/export"
)

// RuntimeAssetsHandler serves embedded frontend helpers (datagrid export client).
func RuntimeAssetsHandler() http.Handler {
	return http.FileServer(http.FS(export.RuntimeAssetsFS()))
}

// PDFAssetsHandler serves embedded PDF preview assets (echarts, jsPDF, html2canvas).
func PDFAssetsHandler(prefix string) http.Handler {
	if prefix == "" {
		prefix = export.DefaultPDFAssetsPath
	}
	prefix = ensureTrailingSlash(prefix)
	return http.StripPrefix(prefix, http.FileServer(http.FS(export.PDFAssetsFS())))
}

func ensureTrailingSlash(value string) string {
	if value == "" {
		return ""
	}
	if value[len(value)-1] == '/' {
		return value
	}
	return value + "/"
}
