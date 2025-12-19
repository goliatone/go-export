package export

import (
	"context"
	"sort"
	"time"
)

// RetentionRule defines a TTL override for matching exports.
type RetentionRule struct {
	Definition string
	Format     Format
	Role       string
	TTL        time.Duration
}

// RetentionRules configures TTL lookups for exports.
type RetentionRules struct {
	DefaultTTL   time.Duration
	ByDefinition map[string]time.Duration
	ByFormat     map[Format]time.Duration
	ByRole       map[string]time.Duration
	Rules        []RetentionRule
	RoleResolver func(actor Actor) []string
}

// TTL returns a TTL for the provided request.
func (r RetentionRules) TTL(ctx context.Context, actor Actor, req ExportRequest, def ResolvedDefinition) (time.Duration, error) {
	_ = ctx
	roles := r.resolveRoles(actor)

	if ttl, ok := matchRetentionRules(r.Rules, def.Name, req.Format, roles); ok {
		return ttl, nil
	}
	if ttl, ok := r.ByDefinition[def.Name]; ok {
		return ttl, nil
	}
	if ttl, ok := r.ByFormat[req.Format]; ok {
		return ttl, nil
	}
	if len(roles) > 0 {
		for _, role := range roles {
			if ttl, ok := r.ByRole[role]; ok {
				return ttl, nil
			}
		}
	}
	return r.DefaultTTL, nil
}

func (r RetentionRules) resolveRoles(actor Actor) []string {
	if r.RoleResolver != nil {
		return r.RoleResolver(actor)
	}
	roles := append([]string(nil), actor.Roles...)
	if len(roles) == 0 && actor.Details != nil {
		if role, ok := actor.Details["role"].(string); ok && role != "" {
			roles = append(roles, role)
		}
		if raw, ok := actor.Details["roles"]; ok {
			switch v := raw.(type) {
			case []string:
				roles = append(roles, v...)
			case []any:
				for _, item := range v {
					if role, ok := item.(string); ok && role != "" {
						roles = append(roles, role)
					}
				}
			}
		}
	}
	return uniqueRoles(roles)
}

func matchRetentionRules(rules []RetentionRule, definition string, format Format, roles []string) (time.Duration, bool) {
	type match struct {
		ttl   time.Duration
		score int
		index int
	}
	var matches []match
	for idx, rule := range rules {
		if rule.Definition != "" && rule.Definition != definition {
			continue
		}
		if rule.Format != "" && rule.Format != format {
			continue
		}
		if rule.Role != "" && !roleMatch(rule.Role, roles) {
			continue
		}
		score := 0
		if rule.Definition != "" {
			score += 4
		}
		if rule.Format != "" {
			score += 2
		}
		if rule.Role != "" {
			score += 1
		}
		matches = append(matches, match{ttl: rule.TTL, score: score, index: idx})
	}
	if len(matches) == 0 {
		return 0, false
	}
	sort.SliceStable(matches, func(i, j int) bool {
		if matches[i].score == matches[j].score {
			return matches[i].index < matches[j].index
		}
		return matches[i].score > matches[j].score
	})
	return matches[0].ttl, true
}

func roleMatch(role string, roles []string) bool {
	for _, candidate := range roles {
		if candidate == role {
			return true
		}
	}
	return false
}

func uniqueRoles(roles []string) []string {
	if len(roles) == 0 {
		return roles
	}
	seen := make(map[string]struct{}, len(roles))
	result := make([]string, 0, len(roles))
	for _, role := range roles {
		if role == "" {
			continue
		}
		if _, ok := seen[role]; ok {
			continue
		}
		seen[role] = struct{}{}
		result = append(result, role)
	}
	return result
}
