package exporthttp

import (
	"net/http"

	"github.com/goliatone/go-export/export"
)

// RuntimeAssetsHandler serves embedded frontend helpers (datagrid export client).
func RuntimeAssetsHandler() http.Handler {
	return http.FileServer(http.FS(export.RuntimeAssetsFS()))
}
