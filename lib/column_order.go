package lib

import (
	"fmt"
	"strings"
)

// ColumnOrderChange compares a table's column order saved with its cursor
// (saved) with the table's current column order (current) and returns how
// they differ, or "" if they are the same. The reason is only logged.
//
// Without the vttablet schema historian (--track-schema-versions), vttablet
// decodes a row event against the table's current schema by position, cut
// down to the event's column count, so any schema change between the cursor
// and the current position puts events from before the change at risk: a new
// column before an existing one, columns reordered, renamed, or dropped and
// added shift values into the wrong columns, and a column added at the end or
// dropped makes vttablet fail on the older events. Every difference is
// therefore a change: the caller streams the table only if the schema tracker
// recorded it (or the cursor is already past it), and otherwise raises the
// re-sync Task.
//
// Column names are compared case-insensitively, like MySQL does. An empty
// saved order (state written by an older connector version, or a table that
// has not been synced yet) is never a change.
func ColumnOrderChange(saved, current []string) string {
	if len(saved) == 0 || len(current) == 0 {
		return ""
	}
	savedIndex := make(map[string]int, len(saved))
	for i, c := range saved {
		savedIndex[strings.ToLower(c)] = i
	}
	currentSet := make(map[string]bool, len(current))
	for _, c := range current {
		currentSet[strings.ToLower(c)] = true
	}

	var dropped []string
	for _, c := range saved {
		if !currentSet[strings.ToLower(c)] {
			dropped = append(dropped, c)
		}
	}

	// Walk the current order for the most telling reason: existing columns
	// out of their relative order, or a new column before an existing one.
	var (
		added        []string
		lastKept     = -1
		lastKeptName string
		firstAdded   string
	)
	for _, c := range current {
		i, ok := savedIndex[strings.ToLower(c)]
		if !ok {
			added = append(added, c)
			if firstAdded == "" {
				firstAdded = c
			}
			continue
		}
		if i < lastKept {
			return fmt.Sprintf("column %s moved before column %s", c, lastKeptName)
		}
		if firstAdded != "" {
			return fmt.Sprintf("new column %s was added before existing column %s", firstAdded, c)
		}
		lastKept, lastKeptName = i, c
	}
	switch {
	case len(dropped) > 0 && len(added) > 0:
		return fmt.Sprintf("column(s) %s dropped and column(s) %s added", strings.Join(dropped, ","), strings.Join(added, ","))
	case len(added) > 0:
		return fmt.Sprintf("new column(s) %s added at the end", strings.Join(added, ","))
	case len(dropped) > 0:
		return fmt.Sprintf("column(s) %s dropped", strings.Join(dropped, ","))
	}
	return ""
}
