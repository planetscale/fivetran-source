package lib

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"vitess.io/vitess/go/mysql/replication"
)

// TrackedStepOver lets Read leave columns out of the VStream filter while it
// replays one shard's binlog events written before those columns were added,
// in a range that the schema history scan verified as tracked
// (SchemaHistoryTracked).
//
// With track_schema_versions on, vttablet's historian decodes an older row
// event with the table's schema at the event's position (the schema version
// recorded at the position of each DDL), and resolves the VStream filter's
// column names against that schema. A column added by a later schema change
// is not in it, so vttablet fails with "column c not found in table t" (see
// VStreamMissingFilterColumn), and the generic re-sync Task would follow. The
// older events never had that column, so leaving it out loses nothing, and
// the other columns are matched by name, not position.
//
// Read only uses it when every condition holds:
//   - the read starts on Shard at Cursor, the cursor the scan started from,
//     out of the copy phase;
//   - vttablet reports a column in Columns (the caller only lists columns
//     added on this shard since the saved column order, with no rename or
//     drop in the same change, nullable, without a default, not generated,
//     not auto-increment and not part of the primary key, so every existing
//     row has NULL in it and nothing was backfilled without row events);
//   - the stream position is still before VerifiedTo;
//   - each column is left out at most once, and at most maxTrackedStepOvers
//     cut-overs are stepped over per Read.
//
// The reduced stream then stops at the first DDL event for the table (the
// Online DDL cut-over) and restarts there with every column. The schema
// tracker records each version at its DDL's position, and the scan saw that
// VERSION event before VerifiedTo, so it is in _vt.schema_version: Vitess v24
// reloads the historian when a stream starts, and older versions use the
// current schema past the last version they loaded. Should vttablet still
// report the same column after the cut-over, it is not left out again. A DDL
// past VerifiedTo, or a reduced stream that ends without one, is not
// something the scan verified: Read returns the original vttablet error
// (re-sync Task).
type TrackedStepOver struct {
	Shard      string
	Cursor     string
	VerifiedTo string
	Columns    []string
}

type trackedStepOverKey struct{}

// WithTrackedStepOver returns a context in which Read may step over the
// column additions described by s (see TrackedStepOver).
func WithTrackedStepOver(ctx context.Context, s TrackedStepOver) context.Context {
	return context.WithValue(ctx, trackedStepOverKey{}, s)
}

// TrackedStepOverFrom returns the TrackedStepOver of ctx, if any.
func TrackedStepOverFrom(ctx context.Context) (TrackedStepOver, bool) {
	s, ok := ctx.Value(trackedStepOverKey{}).(TrackedStepOver)
	return s, ok
}

func (s TrackedStepOver) allows(column string) bool {
	for _, c := range s.Columns {
		if strings.EqualFold(c, column) {
			return true
		}
	}
	return false
}

// maxTrackedStepOvers bounds how many cut-overs one Read steps over. Each
// cut-over may add several columns (vttablet reports them one at a time), but
// it counts once.
var maxTrackedStepOvers = 3

// errPassedSchemaChange ends a sync that streamed with a reduced column list
// at the first DDL event for the table; Read then decides whether to restart
// there with every column.
var errPassedSchemaChange = errors.New("vstream reached a schema change for the table")

// positionAtLeast reports whether binlog position a contains b (GTID sets).
// Positions that can't be decoded are an error: the caller then does not
// step over anything.
func positionAtLeast(a, b string) (bool, error) {
	pa, err := replication.DecodePosition(a)
	if err != nil {
		return false, errors.Wrapf(err, "decoding binlog position %q", a)
	}
	pb, err := replication.DecodePosition(b)
	if err != nil {
		return false, errors.Wrapf(err, "decoding binlog position %q", b)
	}
	if pa.IsZero() || pb.IsZero() {
		return false, fmt.Errorf("empty binlog position (%q, %q)", a, b)
	}
	return pa.AtLeast(pb), nil
}

var vstreamMissingColumnPattern = regexp.MustCompile("column (`(?:[^`]|``)+`|[^\\s`]+) not found in table ")

// VStreamMissingFilterColumn returns the column that vttablet could not find
// when it built the replication plan for an event, e.g.
//
//	failed to build table replication plan for table t: column c not found in table t
//
// With the schema tracker on, this happens when the VStream filter names a
// column that was added after the event was written.
func VStreamMissingFilterColumn(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	message := err.Error()
	if !strings.Contains(message, "failed to build table replication plan") {
		return "", false
	}
	m := vstreamMissingColumnPattern.FindStringSubmatch(message)
	if m == nil {
		return "", false
	}
	column := m[1]
	if strings.HasPrefix(column, "`") {
		column = strings.ReplaceAll(column[1:len(column)-1], "``", "`")
	}
	return column, true
}

// withoutColumn returns columns without column (case-insensitive, like MySQL
// column names) and whether it was there.
func withoutColumn(columns []string, column string) ([]string, bool) {
	reduced := make([]string, 0, len(columns))
	removed := false
	for _, c := range columns {
		if strings.EqualFold(strings.TrimSpace(c), column) {
			removed = true
			continue
		}
		reduced = append(reduced, c)
	}
	return reduced, removed
}

// ColumnDetail is one row of SHOW COLUMNS.
type ColumnDetail struct {
	Name     string
	Nullable bool
	// Default is nil when the column has no default (or DEFAULT NULL).
	Default *string
	Key     string
	Extra   string
}

// StepOverBlocker returns why the column, added by a schema change, must not
// be left out of older binlog events, or "" if it may. Online DDL fills a new
// column for the existing rows on the shadow table, which produces no row
// events for the table, so leaving it out is only lossless when every
// existing row has NULL in it.
func (c ColumnDetail) StepOverBlocker() string {
	extra := strings.TrimSpace(strings.ReplaceAll(strings.ToUpper(c.Extra), "INVISIBLE", ""))
	switch {
	case !c.Nullable:
		return "it is NOT NULL, so existing rows got a value"
	case c.Default != nil:
		return "it has a default, so existing rows got a value"
	case extra != "":
		return fmt.Sprintf("it is %s, so existing rows got a value", strings.ToLower(extra))
	case strings.EqualFold(c.Key, "PRI"):
		return "it is part of the primary key"
	default:
		return ""
	}
}

// AddedColumns returns the columns of current that are not in saved, in
// current's order, and whether every column of saved is still in current
// (case-insensitive). A change that also drops or renames a column is not a
// plain addition: a renamed column keeps its values, which the older events
// carry under the old name.
func AddedColumns(saved, current []string) (added []string, onlyAdded bool) {
	in := func(list []string, name string) bool {
		for _, c := range list {
			if strings.EqualFold(c, name) {
				return true
			}
		}
		return false
	}
	for _, c := range saved {
		if !in(current, c) {
			return nil, false
		}
	}
	for _, c := range current {
		if !in(saved, c) {
			added = append(added, c)
		}
	}
	return added, true
}
