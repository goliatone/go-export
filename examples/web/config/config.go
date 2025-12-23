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
	Template       TemplateConfig
	PDF            PDFConfig
	Notifications  NotificationConfig
}

// TemplateConfig holds template renderer settings.
type TemplateConfig struct {
	Enabled      bool
	TemplateDir  string
	TemplateName string
	MaxRows      int
}

// PDFConfig holds PDF renderer settings.
type PDFConfig struct {
	Enabled              bool
	Engine               string
	WKHTMLTOPDFPath      string
	ChromiumPath         string
	Headless             bool
	Args                 []string
	Timeout              int // seconds
	PageSize             string
	PrintBackground      bool
	PreferCSSPageSize    bool
	Scale                float64
	MarginTop            string
	MarginBottom         string
	MarginLeft           string
	MarginRight          string
	BaseURL              string
	ExternalAssetsPolicy string
}

// NotificationConfig holds export-ready notification settings.
type NotificationConfig struct {
	Enabled    bool
	Recipients []string
	Channels   []string
	SMTP       SMTPConfig
}

// SMTPConfig holds SMTP adapter settings for notifications.
type SMTPConfig struct {
	Host          string
	Port          int
	From          string
	Username      string
	Password      string
	UseTLS        bool
	UseStartTLS   bool
	SkipTLSVerify bool
	AuthDisabled  bool
	PlainOnly     bool
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
			Template: TemplateConfig{
				Enabled:      true,
				TemplateDir:  "./templates/export",
				TemplateName: "export",
				MaxRows:      10000,
			},
			PDF: PDFConfig{
				Enabled:              false, // disabled by default; requires Chromium or wkhtmltopdf
				Engine:               "chromium",
				WKHTMLTOPDFPath:      "wkhtmltopdf",
				ChromiumPath:         "",
				Headless:             true,
				Args:                 []string{"--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"},
				Timeout:              60,
				PageSize:             "",
				PrintBackground:      true,
				PreferCSSPageSize:    true,
				Scale:                1.0,
				MarginTop:            "",
				MarginBottom:         "",
				MarginLeft:           "",
				MarginRight:          "",
				BaseURL:              "",
				ExternalAssetsPolicy: "",
			},
			Notifications: NotificationConfig{
				Enabled:    false,
				Recipients: nil,
				Channels:   nil,
				SMTP: SMTPConfig{
					UseStartTLS: true,
				},
			},
		},
		Features: FeatureFlags{
			EnableAuth: false,
		},
	}
}
