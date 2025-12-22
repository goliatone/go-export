package export

func sanitizeRequestForRecord(req ExportRequest) ExportRequest {
	req.Output = nil
	req.IdempotencyKey = ""
	return req
}
