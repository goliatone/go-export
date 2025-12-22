package main

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/goliatone/go-export/export"
	exportcrud "github.com/goliatone/go-export/sources/crud"
)

var defaultUserColumns = []string{"id", "email", "name", "role", "created_at"}

// UserStreamer adapts the demo users slice to the crud contract.
type UserStreamer struct{}

// Stream filters, sorts, and paginates demo users based on the crud spec.
func (UserStreamer) Stream(ctx context.Context, spec exportcrud.Spec) (export.RowIterator, error) {
	_ = ctx
	users := make([]DemoUser, len(demoUsers))
	copy(users, demoUsers)

	users = applyUserSelection(users, spec.Selection)
	users = applyUserFilters(users, spec.Query.Filters)
	users = applyUserSearch(users, spec.Query.Search)
	users = applyUserSort(users, spec.Query.Sort)
	users = applyUserPaging(users, spec.Query.Offset, spec.Query.Limit)

	columns := spec.Columns
	if len(columns) == 0 {
		columns = defaultUserColumns
	}

	return &userRowIterator{users: users, columns: columns}, nil
}

type userRowIterator struct {
	users   []DemoUser
	columns []string
	idx     int
}

func (it *userRowIterator) Next(ctx context.Context) (export.Row, error) {
	if it.idx >= len(it.users) {
		return nil, io.EOF
	}
	user := it.users[it.idx]
	it.idx++
	return buildUserRow(user, it.columns), nil
}

func (it *userRowIterator) Close() error {
	return nil
}

func applyUserSelection(users []DemoUser, selection export.Selection) []DemoUser {
	if selection.Mode != export.SelectionIDs || len(selection.IDs) == 0 {
		return users
	}
	allowed := make(map[string]struct{}, len(selection.IDs))
	for _, id := range selection.IDs {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
			continue
		}
		allowed[trimmed] = struct{}{}
	}
	filtered := make([]DemoUser, 0, len(users))
	for _, user := range users {
		if _, ok := allowed[user.ID]; ok {
			filtered = append(filtered, user)
		}
	}
	return filtered
}

func applyUserFilters(users []DemoUser, filters []exportcrud.Filter) []DemoUser {
	if len(filters) == 0 {
		return users
	}
	filtered := make([]DemoUser, 0, len(users))
	for _, user := range users {
		if matchesUserFilters(user, filters) {
			filtered = append(filtered, user)
		}
	}
	return filtered
}

func matchesUserFilters(user DemoUser, filters []exportcrud.Filter) bool {
	for _, filter := range filters {
		if !matchesUserFilter(user, filter) {
			return false
		}
	}
	return true
}

func matchesUserFilter(user DemoUser, filter exportcrud.Filter) bool {
	field := strings.ToLower(strings.TrimSpace(filter.Field))
	if field == "" {
		return true
	}
	value := strings.ToLower(strings.TrimSpace(fmt.Sprint(filter.Value)))
	op := strings.ToLower(strings.TrimSpace(filter.Op))
	if op == "" {
		op = "eq"
	}

	switch field {
	case "id":
		return matchString(user.ID, value, op)
	case "email":
		return matchString(user.Email, value, op)
	case "name":
		return matchString(user.Name, value, op)
	case "role":
		return matchString(user.Role, value, op)
	default:
		return true
	}
}

func matchString(actual string, expected string, op string) bool {
	actual = strings.ToLower(actual)
	switch op {
	case "eq":
		return actual == expected
	case "neq":
		return actual != expected
	case "contains":
		return strings.Contains(actual, expected)
	default:
		return true
	}
}

func applyUserSearch(users []DemoUser, search string) []DemoUser {
	search = strings.ToLower(strings.TrimSpace(search))
	if search == "" {
		return users
	}
	filtered := make([]DemoUser, 0, len(users))
	for _, user := range users {
		if matchesUserSearch(user, search) {
			filtered = append(filtered, user)
		}
	}
	return filtered
}

func matchesUserSearch(user DemoUser, search string) bool {
	return strings.Contains(strings.ToLower(user.ID), search) ||
		strings.Contains(strings.ToLower(user.Email), search) ||
		strings.Contains(strings.ToLower(user.Name), search) ||
		strings.Contains(strings.ToLower(user.Role), search)
}

func applyUserSort(users []DemoUser, sorts []exportcrud.Sort) []DemoUser {
	if len(sorts) == 0 {
		return users
	}
	sort.SliceStable(users, func(i, j int) bool {
		left := users[i]
		right := users[j]
		for _, sortSpec := range sorts {
			cmp := compareUserField(left, right, sortSpec.Field)
			if cmp == 0 {
				continue
			}
			if sortSpec.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
	return users
}

func compareUserField(a DemoUser, b DemoUser, field string) int {
	switch strings.ToLower(strings.TrimSpace(field)) {
	case "id":
		return strings.Compare(a.ID, b.ID)
	case "email":
		return strings.Compare(a.Email, b.Email)
	case "name":
		return strings.Compare(a.Name, b.Name)
	case "role":
		return strings.Compare(a.Role, b.Role)
	case "created_at":
		return compareTime(a.CreatedAt, b.CreatedAt)
	default:
		return 0
	}
}

func compareTime(a time.Time, b time.Time) int {
	if a.Before(b) {
		return -1
	}
	if a.After(b) {
		return 1
	}
	return 0
}

func applyUserPaging(users []DemoUser, offset int, limit int) []DemoUser {
	if offset < 0 {
		offset = 0
	}
	if offset >= len(users) {
		return []DemoUser{}
	}
	users = users[offset:]
	if limit <= 0 || limit >= len(users) {
		return users
	}
	return users[:limit]
}

func buildUserRow(user DemoUser, columns []string) export.Row {
	row := make(export.Row, len(columns))
	for idx, column := range columns {
		switch column {
		case "id":
			row[idx] = user.ID
		case "email":
			row[idx] = user.Email
		case "name":
			row[idx] = user.Name
		case "role":
			row[idx] = user.Role
		case "created_at":
			row[idx] = user.CreatedAt
		default:
			row[idx] = ""
		}
	}
	return row
}
