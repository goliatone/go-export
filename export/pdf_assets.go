package export

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"strings"
)

const (
	// DefaultPDFAssetsPath is the local path used to serve embedded PDF preview assets.
	DefaultPDFAssetsPath = "/exports/assets/pdf/"
	envPDFAssetsCDN      = "GO_EXPORT_PDF_ASSETS_CDN"
)

//go:embed assets/pdf/*
var embeddedPDFAssets embed.FS

// PDFAssetsFS exposes the embedded PDF preview assets.
func PDFAssetsFS() fs.FS {
	sub, err := fs.Sub(embeddedPDFAssets, "assets/pdf")
	if err != nil {
		// This should never happen because the directory is embedded at build time.
		panic(fmt.Errorf("export: failed to prepare embedded PDF assets: %w", err))
	}
	return sub
}

// DefaultPDFAssetsHost returns the default assets host, respecting GO_EXPORT_PDF_ASSETS_CDN if set.
func DefaultPDFAssetsHost() string {
	if host := strings.TrimSpace(os.Getenv(envPDFAssetsCDN)); host != "" {
		return ensureTrailingSlash(host)
	}
	return DefaultPDFAssetsPath
}

func ensureTrailingSlash(value string) string {
	if value == "" {
		return ""
	}
	if strings.HasSuffix(value, "/") {
		return value
	}
	return value + "/"
}
