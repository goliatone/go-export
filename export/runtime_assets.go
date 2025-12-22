package export

import (
	"embed"
	"io/fs"
)

//go:embed assets/*.js
var embeddedRuntimeAssets embed.FS

// RuntimeAssetsFS exposes the embedded frontend helpers (datagrid export client).
//
// Typical mount:
//
//	mux.Handle("/assets/",
//		http.StripPrefix("/assets/",
//			http.FileServer(http.FS(export.RuntimeAssetsFS())),
//		),
//	)
func RuntimeAssetsFS() fs.FS {
	sub, err := fs.Sub(embeddedRuntimeAssets, "assets")
	if err != nil {
		return embeddedRuntimeAssets
	}
	return sub
}
