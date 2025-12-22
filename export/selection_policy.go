package export

import "context"

func applySelectionDefaults(ctx context.Context, actor Actor, req ExportRequest, def ResolvedDefinition) (ExportRequest, bool, error) {
	if req.Selection.Mode != "" {
		return req, false, nil
	}

	selection := Selection{}
	applied := false

	if def.SelectionPolicy != nil {
		policySelection, ok, err := def.SelectionPolicy.DefaultSelection(ctx, actor, req, def)
		if err != nil {
			return req, false, err
		}
		if ok {
			selection = policySelection
			applied = true
		}
	}

	if selection.Mode == "" && def.DefaultSelection.Mode != "" {
		selection = def.DefaultSelection
		applied = true
	}

	if selection.Mode == "" {
		selection.Mode = SelectionAll
		applied = true
	}

	req.Selection = selection
	if err := validateSelection(req.Selection); err != nil {
		return req, false, err
	}

	return req, applied, nil
}
