// Package exportsqlite provides a SQLite renderer adapter for go-export.
//
// Renderer is disabled by default; set Renderer.Enabled to true and register it
// on the runner explicitly:
//
//	renderer := exportsqlite.Renderer{Enabled: true}
//	_ = runner.Renderers.Register(export.FormatSQLite, renderer)
//
// Table names are configurable per request via render options
// (render_options.sqlite.table_name). When omitted, the default table name is
// "data".
package exportsqlite
