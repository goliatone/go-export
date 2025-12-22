package export

import "fmt"

func (r *Runner) resolveTransformers(def ResolvedDefinition) ([]RowTransformer, error) {
	if len(def.Transformers) == 0 {
		return nil, nil
	}
	if r.Transformers == nil {
		return nil, NewError(KindInternal, "transformer registry not configured", nil)
	}

	transformers := make([]RowTransformer, 0, len(def.Transformers))
	for _, cfg := range def.Transformers {
		if cfg.Key == "" {
			return nil, NewError(KindValidation, "transformer key is required", nil)
		}
		factory, ok := r.Transformers.Resolve(cfg.Key)
		if !ok {
			return nil, NewError(KindValidation, fmt.Sprintf("transformer %q not registered", cfg.Key), nil)
		}
		if factory.streaming != nil {
			transformer, err := factory.streaming(cfg)
			if err != nil {
				return nil, wrapTransformError(cfg.Key, err)
			}
			if transformer == nil {
				return nil, NewError(KindValidation, fmt.Sprintf("transformer %q is nil", cfg.Key), nil)
			}
			transformers = append(transformers, transformer)
			continue
		}
		if factory.buffered != nil {
			transformer, err := factory.buffered(cfg)
			if err != nil {
				return nil, wrapTransformError(cfg.Key, err)
			}
			if transformer == nil {
				return nil, NewError(KindValidation, fmt.Sprintf("buffered transformer %q is nil", cfg.Key), nil)
			}
			transformers = append(transformers, newBufferedTransformer(transformer, def.Policy))
			continue
		}
		return nil, NewError(KindInternal, fmt.Sprintf("transformer %q is not configured", cfg.Key), nil)
	}

	return transformers, nil
}

func wrapTransformError(key string, err error) error {
	if err == nil {
		return nil
	}
	if exportErr, ok := err.(*ExportError); ok {
		return exportErr
	}
	return NewError(KindValidation, fmt.Sprintf("transformer %q invalid: %v", key, err), err)
}
