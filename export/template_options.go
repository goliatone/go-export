package export

func mergeTemplateOptions(base TemplateOptions, override TemplateOptions) TemplateOptions {
	out := base
	if override.Strategy != "" {
		out.Strategy = override.Strategy
	}
	if override.MaxRows != 0 {
		out.MaxRows = override.MaxRows
	}
	if override.TemplateName != "" {
		out.TemplateName = override.TemplateName
	}
	if override.Layout != "" {
		out.Layout = override.Layout
	}
	if override.Title != "" {
		out.Title = override.Title
	}
	if override.Definition != "" {
		out.Definition = override.Definition
	}
	if !override.GeneratedAt.IsZero() {
		out.GeneratedAt = override.GeneratedAt
	}
	if override.ChartConfig != nil {
		out.ChartConfig = override.ChartConfig
	}
	out.Theme = mergeTemplateMap(out.Theme, override.Theme)
	out.Header = mergeTemplateMap(out.Header, override.Header)
	out.Footer = mergeTemplateMap(out.Footer, override.Footer)
	out.Data = mergeTemplateMap(out.Data, override.Data)
	return out
}

func mergeTemplateMap(base, override map[string]any) map[string]any {
	if base == nil && override == nil {
		return nil
	}
	out := make(map[string]any, len(base)+len(override))
	for key, value := range base {
		out[key] = value
	}
	for key, value := range override {
		out[key] = value
	}
	return out
}
