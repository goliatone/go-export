package export

// SelectDelivery determines the final delivery mode.
func SelectDelivery(req ExportRequest, def ResolvedDefinition, policy DeliveryPolicy) DeliveryMode {
	switch req.Delivery {
	case DeliverySync, DeliveryAsync:
		return req.Delivery
	}

	policy = mergeDeliveryPolicy(policy, def.DeliveryPolicy)

	thresholds := policy.Thresholds
	if thresholds.MaxRows > 0 && req.EstimatedRows > thresholds.MaxRows {
		return DeliveryAsync
	}
	if thresholds.MaxBytes > 0 && req.EstimatedBytes > thresholds.MaxBytes {
		return DeliveryAsync
	}
	if thresholds.MaxDuration > 0 && req.EstimatedDuration > thresholds.MaxDuration {
		return DeliveryAsync
	}

	if policy.Default != "" {
		return policy.Default
	}
	return DeliverySync
}

func mergeDeliveryPolicy(base DeliveryPolicy, override *DeliveryPolicy) DeliveryPolicy {
	merged := base
	if override == nil {
		return merged
	}
	if override.Default != "" {
		merged.Default = override.Default
	}
	if override.Thresholds.MaxRows > 0 {
		merged.Thresholds.MaxRows = override.Thresholds.MaxRows
	}
	if override.Thresholds.MaxBytes > 0 {
		merged.Thresholds.MaxBytes = override.Thresholds.MaxBytes
	}
	if override.Thresholds.MaxDuration > 0 {
		merged.Thresholds.MaxDuration = override.Thresholds.MaxDuration
	}
	return merged
}
