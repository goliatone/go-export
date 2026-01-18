package exportpdf

import (
	"context"
	"errors"
	"fmt"
	"html"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
	"github.com/goliatone/go-export/export"
)

const defaultPDFScale = 1.0

var pdfLengthPattern = regexp.MustCompile(`^\s*([0-9]+(?:\.[0-9]+)?)\s*([a-zA-Z]*)\s*$`)

var pdfPageSizesInches = map[string]struct {
	width  float64
	height float64
}{
	"A3":     {width: 11.69, height: 16.54},
	"A4":     {width: 8.27, height: 11.69},
	"A5":     {width: 5.83, height: 8.27},
	"LETTER": {width: 8.5, height: 11},
	"LEGAL":  {width: 8.5, height: 14},
}

// ChromiumEngine renders PDF output using a shared headless Chromium instance.
type ChromiumEngine struct {
	BrowserPath string
	Headless    bool
	Timeout     time.Duration
	Args        []string

	DefaultPDF export.PDFOptions

	initOnce      sync.Once
	allocCtx      context.Context
	allocCancel   context.CancelFunc
	browserCtx    context.Context
	browserCancel context.CancelFunc
}

// Render executes Chromium-based HTML-to-PDF rendering.
func (e *ChromiumEngine) Render(ctx context.Context, req RenderRequest) ([]byte, error) {
	if e == nil {
		return nil, export.NewError(export.KindInternal, "chromium engine is nil", nil)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	if err := e.ensureBrowser(); err != nil {
		return nil, export.NewError(export.KindInternal, "chromium engine init failed", err)
	}

	tabCtx, cancel := chromedp.NewContext(e.browserCtx)
	defer cancel()

	execCtx := tabCtx
	if ctx != nil {
		var cancelReq context.CancelFunc
		execCtx, cancelReq = context.WithCancel(tabCtx)
		defer cancelReq()
		go func() {
			select {
			case <-ctx.Done():
				cancelReq()
			case <-execCtx.Done():
			}
		}()
	}
	if e.Timeout > 0 {
		var cancelTimeout context.CancelFunc
		execCtx, cancelTimeout = context.WithTimeout(execCtx, e.Timeout)
		defer cancelTimeout()
	}

	options := mergePDFOptions(e.defaultPDFOptions(), req.Options.PDF)
	htmlInput := injectBaseURL(req.HTML, options.BaseURL)

	var pdf []byte
	actions := []chromedp.Action{}
	if options.ExternalAssetsPolicy == export.PDFExternalAssetsBlock {
		actions = append(actions,
			network.Enable(),
			network.SetBlockedURLs([]string{"http://*", "https://*"}),
		)
	}

	actions = append(actions,
		chromedp.Navigate("about:blank"),
		chromedp.ActionFunc(func(ctx context.Context) error {
			tree, err := page.GetFrameTree().Do(ctx)
			if err != nil {
				return err
			}
			return page.SetDocumentContent(tree.Frame.ID, string(htmlInput)).Do(ctx)
		}),
		chromedp.WaitReady("body", chromedp.ByQuery),
		chromedp.ActionFunc(func(ctx context.Context) error {
			params, err := buildPrintToPDFParams(options)
			if err != nil {
				return err
			}
			pdf, _, err = params.Do(ctx)
			return err
		}),
	)

	if err := chromedp.Run(execCtx, actions...); err != nil {
		return nil, export.NewError(export.KindInternal, "chromium pdf render failed", err)
	}
	return pdf, nil
}

// Close releases Chromium resources if they have been initialized.
func (e *ChromiumEngine) Close() error {
	if e == nil {
		return nil
	}
	if e.browserCancel != nil {
		e.browserCancel()
	}
	if e.allocCancel != nil {
		e.allocCancel()
	}
	return nil
}

func (e *ChromiumEngine) ensureBrowser() error {
	e.initOnce.Do(func() {
		options := append([]chromedp.ExecAllocatorOption{}, chromedp.DefaultExecAllocatorOptions[:]...)
		if e.BrowserPath != "" {
			options = append(options, chromedp.ExecPath(e.BrowserPath))
		}
		options = append(options, chromedp.Flag("headless", e.Headless))
		options = append(options, allocatorOptionsFromArgs(e.Args)...)

		e.allocCtx, e.allocCancel = chromedp.NewExecAllocator(context.Background(), options...)
		e.browserCtx, e.browserCancel = chromedp.NewContext(e.allocCtx)
	})
	if e.allocCtx == nil || e.browserCtx == nil {
		return errors.New("chromium allocator unavailable")
	}
	return nil
}

func (e *ChromiumEngine) defaultPDFOptions() export.PDFOptions {
	defaults := e.DefaultPDF
	if defaults.Scale == 0 {
		defaults.Scale = defaultPDFScale
	}
	if defaults.PrintBackground == nil {
		defaults.PrintBackground = boolPtr(true)
	}
	return defaults
}

func mergePDFOptions(base, override export.PDFOptions) export.PDFOptions {
	merged := base
	if override.PageSize != "" {
		merged.PageSize = override.PageSize
	}
	if override.Landscape != nil {
		merged.Landscape = override.Landscape
	}
	if override.PrintBackground != nil {
		merged.PrintBackground = override.PrintBackground
	}
	if override.Scale != 0 {
		merged.Scale = override.Scale
	}
	if override.MarginTop != "" {
		merged.MarginTop = override.MarginTop
	}
	if override.MarginBottom != "" {
		merged.MarginBottom = override.MarginBottom
	}
	if override.MarginLeft != "" {
		merged.MarginLeft = override.MarginLeft
	}
	if override.MarginRight != "" {
		merged.MarginRight = override.MarginRight
	}
	if override.PreferCSSPageSize != nil {
		merged.PreferCSSPageSize = override.PreferCSSPageSize
	}
	if override.BaseURL != "" {
		merged.BaseURL = override.BaseURL
	}
	if override.ExternalAssetsPolicy != "" {
		merged.ExternalAssetsPolicy = override.ExternalAssetsPolicy
	}
	return merged
}

func buildPrintToPDFParams(opts export.PDFOptions) (*page.PrintToPDFParams, error) {
	params := page.PrintToPDF()

	scale := opts.Scale
	if scale == 0 {
		scale = defaultPDFScale
	}
	if scale < 0.1 || scale > 2.0 {
		return nil, export.NewError(export.KindValidation, "pdf scale must be between 0.1 and 2.0", nil)
	}
	params = params.WithScale(scale)

	if opts.Landscape != nil {
		params = params.WithLandscape(*opts.Landscape)
	}
	if opts.PrintBackground != nil {
		params = params.WithPrintBackground(*opts.PrintBackground)
	}

	preferCSS := false
	if opts.PreferCSSPageSize != nil {
		preferCSS = *opts.PreferCSSPageSize
	} else if opts.PageSize == "" {
		preferCSS = true
	}
	if preferCSS {
		params = params.WithPreferCSSPageSize(true)
	}

	if opts.PageSize != "" {
		size, ok := pdfPageSizesInches[strings.ToUpper(opts.PageSize)]
		if !ok {
			return nil, export.NewError(export.KindValidation, fmt.Sprintf("unsupported pdf page size: %s", opts.PageSize), nil)
		}
		params = params.WithPaperWidth(size.width).WithPaperHeight(size.height)
	}

	if opts.MarginTop != "" {
		value, err := parseLengthInches(opts.MarginTop)
		if err != nil {
			return nil, err
		}
		params = params.WithMarginTop(value)
	}
	if opts.MarginBottom != "" {
		value, err := parseLengthInches(opts.MarginBottom)
		if err != nil {
			return nil, err
		}
		params = params.WithMarginBottom(value)
	}
	if opts.MarginLeft != "" {
		value, err := parseLengthInches(opts.MarginLeft)
		if err != nil {
			return nil, err
		}
		params = params.WithMarginLeft(value)
	}
	if opts.MarginRight != "" {
		value, err := parseLengthInches(opts.MarginRight)
		if err != nil {
			return nil, err
		}
		params = params.WithMarginRight(value)
	}

	return params, nil
}

func parseLengthInches(value string) (float64, error) {
	matches := pdfLengthPattern.FindStringSubmatch(value)
	if len(matches) != 3 {
		return 0, export.NewError(export.KindValidation, fmt.Sprintf("invalid pdf length: %s", value), nil)
	}

	raw := matches[1]
	unit := strings.ToLower(matches[2])
	if unit == "" {
		unit = "in"
	}

	amount, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, export.NewError(export.KindValidation, fmt.Sprintf("invalid pdf length: %s", value), err)
	}

	switch unit {
	case "in":
		return amount, nil
	case "cm":
		return amount / 2.54, nil
	case "mm":
		return amount / 25.4, nil
	case "pt":
		return amount / 72.0, nil
	case "px":
		return amount / 96.0, nil
	default:
		return 0, export.NewError(export.KindValidation, fmt.Sprintf("unsupported pdf length unit: %s", unit), nil)
	}
}

func injectBaseURL(htmlInput []byte, baseURL string) []byte {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return htmlInput
	}

	lower := strings.ToLower(string(htmlInput))
	if strings.Contains(lower, "<base") {
		return htmlInput
	}

	baseTag := fmt.Sprintf(`<base href="%s">`, html.EscapeString(baseURL))
	if headIdx := strings.Index(lower, "<head"); headIdx >= 0 {
		if end := strings.Index(lower[headIdx:], ">"); end >= 0 {
			insertPos := headIdx + end + 1
			return append(append([]byte{}, htmlInput[:insertPos]...), append([]byte(baseTag), htmlInput[insertPos:]...)...)
		}
	}

	if htmlIdx := strings.Index(lower, "<html"); htmlIdx >= 0 {
		if end := strings.Index(lower[htmlIdx:], ">"); end >= 0 {
			insertPos := htmlIdx + end + 1
			injected := fmt.Sprintf("<head>%s</head>", baseTag)
			return append(append([]byte{}, htmlInput[:insertPos]...), append([]byte(injected), htmlInput[insertPos:]...)...)
		}
	}

	return append([]byte(baseTag), htmlInput...)
}

func allocatorOptionsFromArgs(args []string) []chromedp.ExecAllocatorOption {
	options := make([]chromedp.ExecAllocatorOption, 0, len(args))
	for _, arg := range args {
		arg = strings.TrimSpace(arg)
		if arg == "" {
			continue
		}
		arg = strings.TrimPrefix(arg, "--")
		if arg == "" {
			continue
		}
		if name, value, ok := strings.Cut(arg, "="); ok {
			options = append(options, chromedp.Flag(name, value))
			continue
		}
		options = append(options, chromedp.Flag(arg, true))
	}
	return options
}

func boolPtr(value bool) *bool {
	return &value
}
