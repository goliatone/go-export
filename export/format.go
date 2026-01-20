package export

import "strings"

// NormalizeFormat coerces format values into known aliases with defaults applied.
func NormalizeFormat(format Format) Format {
	normalized := strings.ToLower(strings.TrimSpace(string(format)))
	switch normalized {
	case "", string(FormatCSV):
		return FormatCSV
	case "excel", "xls":
		return FormatXLSX
	case "sqlite", "sqlite3", "db":
		return FormatSQLite
	default:
		return Format(normalized)
	}
}
