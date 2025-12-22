package config

// Config holds the web example configuration.
type Config struct {
	Server   ServerConfig
	Export   ExportConfig
	Features FeatureFlags
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Host string
	Port string
}

// ExportConfig holds export-specific settings.
type ExportConfig struct {
	ArtifactDir    string
	MaxRows        int
	DefaultFormat  string
	EnableAsync    bool
	CleanupOnStart bool
}

// FeatureFlags toggles optional features.
type FeatureFlags struct {
	EnableAuth bool
}

// Defaults returns a Config with sensible defaults.
func Defaults() Config {
	return Config{
		Server: ServerConfig{
			Host: "localhost",
			Port: "8080",
		},
		Export: ExportConfig{
			ArtifactDir:    "./artifacts",
			MaxRows:        5,
			DefaultFormat:  "csv",
			EnableAsync:    true,
			CleanupOnStart: true,
		},
		Features: FeatureFlags{
			EnableAuth: false,
		},
	}
}
