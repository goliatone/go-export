package exporthttp

// asyncResponse describes async export responses.
type asyncResponse struct {
	ID          string `json:"id"`
	StatusURL   string `json:"status_url"`
	DownloadURL string `json:"download_url"`
}

type errorResponse struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Message string `json:"message"`
	Code    string `json:"code,omitempty"`
}
