package lib

import (
	"context"
	"fmt"
	"testing"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtgateservicepb "vitess.io/vitess/go/vt/proto/vtgateservice"
)

const stepOverUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

// sp is a binlog position of the step-over tests: GTIDs 1-n of one server.
func sp(n int) string { return fmt.Sprintf("MySQL56/%s:1-%d", stepOverUUID, n) }

func spCursor(n int) *psdbconnect.TableCursor {
	return &psdbconnect.TableCursor{Shard: "-", Keyspace: "connect-test", Position: sp(n)}
}

func stepOverMissingColumn(column string) error {
	return status.Error(codes.FailedPrecondition, `persistent error in vstream for connect-test/- on tablet zone1-0000000101; giving up: `+
		`vttablet: rpc error: code = FailedPrecondition desc = failed to build table replication plan for table customers: `+
		`column `+column+` not found in table customers`)
}

var (
	soOldFields = sqltypes.MakeTestFields("id|name", "int64|varbinary")
	soNewFields = sqltypes.MakeTestFields("id|name|added_col", "int64|varbinary|varbinary")
	soOldRow    = sqltypes.ResultToProto3(sqltypes.MakeTestResult(soOldFields, "1|first")).Rows[0]
	soNewRow    = sqltypes.ResultToProto3(sqltypes.MakeTestResult(soNewFields, "2|second|new")).Rows[0]
)

func soRows(fields []*query.Field, row *query.Row) *binlogdatapb.VEvent {
	return &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "connect-test.customers", RowChanges: []*binlogdatapb.RowChange{{After: row}}}}
}

func soFields(fields []*query.Field) *binlogdatapb.VEvent {
	return &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "connect-test.customers", Fields: fields}}
}

func soVGTID(n int) *binlogdatapb.VEvent {
	return vstreamVGtidEventFromCursor("customers", spCursor(n))
}

func soDDL() *binlogdatapb.VEvent {
	return &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_DDL, Statement: "RENAME TABLE `customers` TO `_vt_hld_x_`, `_vt_vrp_y_` TO `customers`, `_vt_hld_x_` TO `_vt_vrp_y_`"}
}

func soErrStream(err error) *vstreamClientMock {
	return &vstreamClientMock{recvFn: func() (*vtgatepb.VStreamResponse, error) { return nil, err }}
}

func soEvents(events ...*binlogdatapb.VEvent) *vtgatepb.VStreamResponse {
	return &vtgatepb.VStreamResponse{Events: events}
}

// soRead runs Read from position 10 with the current position at stop, the
// VStreams given in order (after that, a stream that fails the test), and
// the columns id, name, added_col (all selected).
type soResult struct {
	filters, starts []string
	rows            []*sqltypes.Result
	checkpoints     []string
	cursor          string
	err             error
}

func soRead(t *testing.T, ctx context.Context, stop int, columns []string, streams ...*vstreamClientMock) soResult {
	t.Helper()
	var r soResult
	ped := connectClient{}
	vc := &vstreamConnectionMock{
		vstreamFn: func(ctx context.Context, in *vtgatepb.VStreamRequest, opts ...grpc.CallOption) (vtgateservicepb.Vitess_VStreamClient, error) {
			gtid := in.Vgtid.ShardGtids[0].Gtid
			if gtid == "current" {
				return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{soEvents(soVGTID(stop))}}, nil
			}
			r.filters = append(r.filters, in.Filter.Rules[0].Filter)
			r.starts = append(r.starts, gtid)
			if len(r.filters) > len(streams) {
				t.Fatalf("unexpected VStream %d from %s with %s", len(r.filters), gtid, in.Filter.Rules[0].Filter)
			}
			return streams[len(r.filters)-1], nil
		},
	}
	ped.vstreamClientFn = func(ctx context.Context, ps PlanetScaleSource) (vstreamClient, error) { return vc, nil }
	mysqlClient := NewTestMysqlClient(func(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
		var cols []MysqlColumn
		for _, c := range columns {
			cols = append(cols, MysqlColumn{Name: c, Type: "varchar(64)", IsPrimaryKey: c == "id"})
		}
		return cols, nil
	})
	ped.Mysql = &mysqlClient
	sc, err := ped.Read(ctx, &dbLogger{}, PlanetScaleSource{Database: "connect-test"}, "customers", columns, spCursor(10),
		func(res *sqltypes.Result, _ Operation) error {
			r.rows = append(r.rows, res)
			return nil
		},
		func(cursor *psdbconnect.TableCursor) error {
			r.checkpoints = append(r.checkpoints, cursor.Position)
			return nil
		}, nil)
	r.err = err
	if sc != nil {
		if tc, err := sc.SerializedCursorToTableCursor(); err == nil {
			r.cursor = tc.Position
		}
	}
	return r
}

func soCtx(verifiedTo int, columns ...string) context.Context {
	if len(columns) == 0 {
		columns = []string{"added_col"}
	}
	return WithTrackedStepOver(context.Background(), TrackedStepOver{Shard: "-", Cursor: sp(10), VerifiedTo: sp(verifiedTo), Columns: columns})
}

var (
	soAll     = []string{"id", "name", "added_col"}
	soFull    = "SELECT `id`,`name`,`added_col` FROM `customers`"
	soReduced = "SELECT `id`,`name` FROM `customers`"
)

// The reduced stream: an old-layout write, then the cut-over at position ddl.
func soReducedToDDL(ddl int) *vstreamClientMock {
	return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{
		soEvents(soFields(soOldFields), soRows(soOldFields, soOldRow), soVGTID(11)),
		soEvents(soVGTID(ddl), soDDL()),
		// Must not be read: a write after the cut-over.
		soEvents(soFields(soNewFields), soRows(soNewFields, soNewRow)),
	}}
}

func soFullToStop(stop int) *vstreamClientMock {
	return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{
		soEvents(soFields(soNewFields), soRows(soNewFields, soNewRow), soVGTID(stop)),
		soEvents(soVGTID(stop + 1)),
	}}
}

func fieldNamesOf(fields []*query.Field) []string {
	names := make([]string, 0, len(fields))
	for _, f := range fields {
		names = append(names, f.Name)
	}
	return names
}

// Tracked range up to 20, cut-over at 15: the column is left out until the
// cut-over, the cursor is checkpointed there, and the stream restarts there
// with every column.
func TestRead_TrackedStepOverWithinVerifiedRange(t *testing.T) {
	r := soRead(
		t, soCtx(20), 25, soAll,
		soErrStream(stepOverMissingColumn("added_col")),
		soReducedToDDL(15),
		soFullToStop(25),
	)
	assert.NoError(t, r.err)
	assert.Equal(t, []string{soFull, soReduced, soFull}, r.filters)
	assert.Equal(t, []string{sp(10), sp(10), sp(15)}, r.starts)
	if assert.Len(t, r.rows, 2) {
		assert.Equal(t, []string{"id", "name"}, fieldNamesOf(r.rows[0].Fields))
		assert.Equal(t, []string{"id", "name", "added_col"}, fieldNamesOf(r.rows[1].Fields))
	}
	assert.Contains(t, r.checkpoints, sp(15))
	assert.Equal(t, sp(26), r.cursor)
}

// Each guard keeps the plain behaviour: vttablet's error, which raises the
// re-sync Task, and no stream after the refusal.
func TestRead_TrackedStepOverGuards(t *testing.T) {
	other := func(mod func(*TrackedStepOver)) context.Context {
		s := TrackedStepOver{Shard: "-", Cursor: sp(10), VerifiedTo: sp(20), Columns: []string{"added_col"}}
		mod(&s)
		return WithTrackedStepOver(context.Background(), s)
	}
	for _, tc := range []struct {
		name    string
		ctx     context.Context
		missing string
		streams int // VStreams expected (after the first failing one)
	}{
		{name: "no tracked verdict", ctx: context.Background(), missing: "added_col"},
		{name: "another shard", ctx: other(func(s *TrackedStepOver) { s.Shard = "-80" }), missing: "added_col"},
		{name: "another cursor", ctx: other(func(s *TrackedStepOver) { s.Cursor = sp(9) }), missing: "added_col"},
		{name: "column not cleared", ctx: soCtx(20), missing: "name"},
		{name: "cursor at the end of the verified range", ctx: soCtx(10), missing: "added_col"},
		{name: "cursor past the verified range", ctx: soCtx(8), missing: "added_col"},
		{name: "undecodable verified position", ctx: other(func(s *TrackedStepOver) { s.VerifiedTo = "garbage" }), missing: "added_col"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := soRead(t, tc.ctx, 25, soAll, soErrStream(stepOverMissingColumn(tc.missing)))
			assert.Error(t, r.err)
			assert.True(t, IsVStreamSchemaIncompatibilityError(r.err), r.err)
			assert.Equal(t, []string{soFull}, r.filters)
		})
	}
}

// A cut-over past the verified range was not checked by the scan: the Task,
// and the cursor is not checkpointed at the unverified DDL.
func TestRead_TrackedStepOverRefusesDDLPastVerifiedRange(t *testing.T) {
	r := soRead(
		t, soCtx(14), 25, soAll,
		soErrStream(stepOverMissingColumn("added_col")),
		soReducedToDDL(15),
	)
	assert.Error(t, r.err)
	assert.True(t, IsVStreamSchemaIncompatibilityError(r.err), r.err)
	assert.Equal(t, []string{soFull, soReduced}, r.filters)
	assert.NotContains(t, r.checkpoints, sp(15))
}

// A reduced stream that gets to the current position without the cut-over
// never saw the schema change that added the column: the Task.
func TestRead_TrackedStepOverRefusesWithoutDDL(t *testing.T) {
	r := soRead(
		t, soCtx(30), 25, soAll,
		soErrStream(stepOverMissingColumn("added_col")),
		&vstreamClientMock{responses: []*vtgatepb.VStreamResponse{
			soEvents(soFields(soOldFields), soRows(soOldFields, soOldRow), soVGTID(25)),
			soEvents(soVGTID(26)),
		}},
	)
	assert.Error(t, r.err)
	assert.True(t, IsVStreamSchemaIncompatibilityError(r.err), r.err)
	assert.Equal(t, []string{soFull, soReduced}, r.filters)
}

// The same column missing again after the cut-over (vttablet still decodes
// with the schema from before it) is not stepped over twice.
func TestRead_TrackedStepOverRefusesSameColumnTwice(t *testing.T) {
	r := soRead(
		t, soCtx(20), 25, soAll,
		soErrStream(stepOverMissingColumn("added_col")),
		soReducedToDDL(15),
		soErrStream(stepOverMissingColumn("added_col")),
	)
	assert.Error(t, r.err)
	assert.True(t, IsVStreamSchemaIncompatibilityError(r.err), r.err)
	assert.Equal(t, []string{soFull, soReduced, soFull}, r.filters)
	// The rows before the cut-over were synced and the cut-over checkpointed.
	assert.Contains(t, r.checkpoints, sp(15))
}

// Cut-overs are counted, not columns: one cut-over adding two columns is one
// step-over; a second cut-over past maxTrackedStepOvers is refused.
func TestRead_TrackedStepOverCountsCutOvers(t *testing.T) {
	cols := []string{"id", "name", "a", "b"}
	ctx := WithTrackedStepOver(context.Background(), TrackedStepOver{Shard: "-", Cursor: sp(10), VerifiedTo: sp(20), Columns: []string{"a", "b"}})

	t.Run("two columns in one cut-over", func(t *testing.T) {
		defer func(prev int) { maxTrackedStepOvers = prev }(maxTrackedStepOvers)
		maxTrackedStepOvers = 1
		r := soRead(
			t, ctx, 25, cols,
			soErrStream(stepOverMissingColumn("a")),
			soErrStream(stepOverMissingColumn("b")),
			soReducedToDDL(15),
			soFullToStop(25),
		)
		assert.NoError(t, r.err)
		assert.Equal(t, []string{
			"SELECT `id`,`name`,`a`,`b` FROM `customers`",
			"SELECT `id`,`name`,`b` FROM `customers`",
			"SELECT `id`,`name` FROM `customers`",
			"SELECT `id`,`name`,`a`,`b` FROM `customers`",
		}, r.filters)
	})
	t.Run("second cut-over over the bound", func(t *testing.T) {
		defer func(prev int) { maxTrackedStepOvers = prev }(maxTrackedStepOvers)
		maxTrackedStepOvers = 1
		r := soRead(
			t, ctx, 25, cols,
			soErrStream(stepOverMissingColumn("a")),
			soReducedToDDL(15),
			soErrStream(stepOverMissingColumn("b")),
		)
		assert.Error(t, r.err)
		assert.True(t, IsVStreamSchemaIncompatibilityError(r.err), r.err)
		assert.Len(t, r.filters, 3)
	})
	t.Run("second cut-over within the bound", func(t *testing.T) {
		r := soRead(
			t, ctx, 25, cols,
			soErrStream(stepOverMissingColumn("a")),
			soReducedToDDL(15),
			soErrStream(stepOverMissingColumn("b")),
			&vstreamClientMock{responses: []*vtgatepb.VStreamResponse{soEvents(soVGTID(18), soDDL())}},
			soFullToStop(25),
		)
		assert.NoError(t, r.err)
		assert.Equal(t, []string{sp(10), sp(10), sp(15), sp(15), sp(18)}, r.starts)
	})
}

// Binary logs that expire during the reduced stream: the copy reads every
// column, with no step-over.
func TestRead_TrackedStepOverResetOnBinlogExpiration(t *testing.T) {
	expired := status.Error(codes.Unknown, "Cannot replicate because the source purged required binary logs. Replicate the missing transactions from elsewhere, or provision a new replica from backup.")
	if !IsBinlogsExpirationError(expired) {
		t.Skip("expiration error text not recognised")
	}
	r := soRead(
		t, soCtx(20), 25, soAll,
		soErrStream(stepOverMissingColumn("added_col")),
		soErrStream(expired),
		&vstreamClientMock{responses: []*vtgatepb.VStreamResponse{
			soEvents(&binlogdatapb.VEvent{Type: binlogdatapb.VEventType_COPY_COMPLETED}),
		}},
	)
	assert.Equal(t, []string{soFull, soReduced, soFull}, r.filters)
	assert.Equal(t, "", r.starts[2])
}

func TestVStreamMissingFilterColumn(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		column string
		ok     bool
	}{
		{name: "vttablet error", err: stepOverMissingColumn("added_col"), column: "added_col", ok: true},
		{name: "quoted column name", err: fmt.Errorf("failed to build table replication plan for table t1: column `sel``ect` not found in table t1"), column: "sel`ect", ok: true},
		{name: "positional fields", err: fmt.Errorf("failed to build table replication plan for table t1: cannot use column names in vstream filter")},
		{name: "unrelated error", err: fmt.Errorf("column x not found in table t1")},
		{name: "nil"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			column, ok := VStreamMissingFilterColumn(tc.err)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.column, column)
		})
	}
}

func TestColumnDetailStepOverBlocker(t *testing.T) {
	def := "0"
	for _, tc := range []struct {
		name    string
		detail  ColumnDetail
		blocked bool
	}{
		{name: "nullable, no default", detail: ColumnDetail{Name: "c", Nullable: true}},
		{name: "invisible", detail: ColumnDetail{Name: "c", Nullable: true, Extra: "INVISIBLE"}},
		{name: "not null", detail: ColumnDetail{Name: "c"}, blocked: true},
		{name: "default", detail: ColumnDetail{Name: "c", Nullable: true, Default: &def}, blocked: true},
		{name: "virtual generated", detail: ColumnDetail{Name: "c", Nullable: true, Extra: "VIRTUAL GENERATED"}, blocked: true},
		{name: "stored generated", detail: ColumnDetail{Name: "c", Nullable: true, Extra: "STORED GENERATED"}, blocked: true},
		{name: "auto increment", detail: ColumnDetail{Name: "c", Nullable: true, Extra: "auto_increment"}, blocked: true},
		{name: "primary key", detail: ColumnDetail{Name: "c", Nullable: true, Key: "PRI"}, blocked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.blocked, tc.detail.StepOverBlocker() != "")
		})
	}
}

func TestAddedColumns(t *testing.T) {
	added, only := AddedColumns([]string{"id", "name"}, []string{"id", "c", "NAME", "d"})
	assert.True(t, only)
	assert.Equal(t, []string{"c", "d"}, added)
	_, only = AddedColumns([]string{"id", "name"}, []string{"id", "full_name"})
	assert.False(t, only, "rename")
	_, only = AddedColumns([]string{"id", "name"}, []string{"id"})
	assert.False(t, only, "drop")
}

func TestPositionAtLeast(t *testing.T) {
	ok, err := positionAtLeast(sp(20), sp(15))
	assert.NoError(t, err)
	assert.True(t, ok)
	ok, err = positionAtLeast(sp(15), sp(20))
	assert.NoError(t, err)
	assert.False(t, ok)
	_, err = positionAtLeast("", sp(20))
	assert.Error(t, err)
	_, err = positionAtLeast("garbage", sp(20))
	assert.Error(t, err)
}
