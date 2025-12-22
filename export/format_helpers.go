package export

func isTemplateFormat(format Format) bool {
	return format == FormatTemplate || format == FormatPDF
}
