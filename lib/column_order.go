package lib

import (
	"fmt"
	"strings"
)

// ColumnOrderChange compares a table's column order saved with its cursor
// (saved) with the table's current column order (current) and returns why
// binlog events written before the change can no longer be decoded safely,
// or "" if they can.
//
// Without the vttablet schema historian (--track-schema-versions), vttablet
// decodes a row event against the table's current schema by position, cut
// down to the event's column count. Events from before a schema change only
// decode correctly if the saved order is a prefix of the current order:
//
//   - identical, or new columns appended at the end: fine (a column added at
//     the end is left out of the VStream filter until the cut-over, see Read);
//   - columns only dropped (the rest in the same relative order): left to the
//     existing paths, vttablet fails with "event has more columns than the
//     schema" and the sync raises the re-sync Task;
//   - a new column before an existing one (ADD COLUMN ... AFTER/FIRST),
//     existing columns in a different relative order (MODIFY ... AFTER/FIRST),
//     or columns dropped and added in the same change: the old values would
//     silently land under the wrong column names, so this returns a reason.
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

	// Walk the current order: existing columns must keep their relative
	// order, and every new column must come after the last existing one.
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
	if len(dropped) > 0 && len(added) > 0 {
		return fmt.Sprintf("column(s) %s dropped and column(s) %s added", strings.Join(dropped, ","), strings.Join(added, ","))
	}
	return ""
}
