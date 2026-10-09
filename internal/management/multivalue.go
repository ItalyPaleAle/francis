package management

import (
	"cmp"
	"net/url"
	"slices"
)

// A filter can take several values, such as more than one status, by repeating its query parameter
// Each combination of the filters' values is listed after the previous one, with the provider's own filter and order, so the order never depends on how a provider sorts across values
// The cursor of such a listing records which combination the next page continues in, as its index among the combinations, which are always built in the same order

// valuesPage is a page of a listing across the combinations of filter values
type valuesPage[T any, A any] struct {
	Items []T
	// HasMore is true when more items follow, starting in the combination at index Group, after After
	HasMore bool
	Group   int
	After   A
}

// fetchValue lists the items matching one combination of filter values, from after the given position
// It returns the items, the position to continue from, and whether more items match the combination
type fetchValue[V any, T any, A any] func(value V, after A, limit int) (items []T, next A, hasMore bool, err error)

// pageAcrossValues lists up to limit items across the combinations of filter values, starting in the combination at index group, after the given position
// A combination that runs out within the page hands over to the next one, and a page that fills up exactly where one runs out only reports more items when a later one has some, so the last page is never empty
func pageAcrossValues[V any, T any, A any](values []V, group int, after A, limit int, fetch fetchValue[V, T, A]) (res valuesPage[T, A], err error) {
	var zero A
	for i := group; i < len(values); i++ {
		// Only the combination the cursor stopped in resumes part-way, and the ones after it start from their beginning
		start := zero
		if i == group {
			start = after
		}

		items, next, hasMore, err := fetch(values[i], start, limit-len(res.Items))
		if err != nil {
			return valuesPage[T, A]{}, err
		}
		res.Items = append(res.Items, items...)
		if hasMore {
			res.HasMore = true
			res.Group = i
			res.After = next
			return res, nil
		}
		if len(res.Items) < limit {
			continue
		}

		// The page is full where this combination ran out, so the next page starts in the first later one that matches anything
		for j := i + 1; j < len(values); j++ {
			probe, _, _, err := fetch(values[j], zero, 1)
			if err != nil {
				return valuesPage[T, A]{}, err
			}
			if len(probe) > 0 {
				res.HasMore = true
				res.Group = j
				return res, nil
			}
		}

		return res, nil
	}

	return res, nil
}

// queryValues returns the values of a query parameter that can be repeated, without duplicates or empty values, sorted with compare so every page of a listing sees them in the same order
// Without any value it returns a single empty value, which lists everything as if there was no filter
func queryValues(q url.Values, name string, compare func(a, b string) int) []string {
	values := make([]string, 0, len(q[name]))
	for _, v := range q[name] {
		if v != "" && !slices.Contains(values, v) {
			values = append(values, v)
		}
	}
	if len(values) == 0 {
		return []string{""}
	}

	slices.SortFunc(values, compare)
	return values
}

// inOrder compares values by their position in a list, such as statuses in the order of their lifecycle
// Values that aren't in the list sort first, so callers validate the values before relying on the order
func inOrder[S ~string](list []S) func(a, b string) int {
	return func(a, b string) int {
		return cmp.Compare(slices.Index(list, S(a)), slices.Index(list, S(b)))
	}
}

// validGroup reports whether a cursor's group index points at one of the combinations
func validGroup[V any](group int, values []V) bool {
	return group >= 0 && group < len(values)
}

// requireTypeForID rejects an actor ID filter without an actor type, since an ID is only unique within its type
func requireTypeForID(types []string, ids []string) *apiError {
	if ids[0] != "" && types[0] == "" {
		return errBadRequest("the id filter requires the type filter")
	}

	return nil
}
