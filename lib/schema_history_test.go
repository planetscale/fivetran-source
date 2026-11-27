package lib

import (
	"context"
	"errors"
	"testing"
	"time"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtgateservicepb "vitess.io/vitess/go/vt/proto/vtgateservice"
)

const (
	shKeyspace = "ks"
	shStart    = "MySQL56/a:1-10"
	shStop     = "MySQL56/a:1-20"
	shDDLTime  = int64(1_700_000_000)
	shRename   = "RENAME TABLE `t` TO `_vt_hld_x_`, `_vt_vrp_y_` TO `t`, `_vt_hld_x_` TO `_vt_vrp_y_`"
)

func shVGTID(shard, pos string) *binlogdatapb.VEvent {
	return &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_VGTID, Vgtid: &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: shKeyspace, Shard: shard, Gtid: pos}},
	}}
}

func shDDL(ts int64) *binlogdatapb.VEvent {
	return &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_DDL, Statement: shRename, Timestamp: ts}
}

func shVersion(ts int64) *binlogdatapb.VEvent {
	return &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_VERSION, Timestamp: ts}
}

func shRow(ts int64) *binlogdatapb.VEvent {
	return &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_ROW, Timestamp: ts, RowEvent: &binlogdatapb.RowEvent{TableName: "t"}}
}

func shResp(events ...*binlogdatapb.VEvent) *vtgatepb.VStreamResponse {
	return &vtgatepb.VStreamResponse{Events: events}
}

// shRecv returns the responses in order, then blocks until ctx is done, like
// a live VStream at the current position.
func shRecv(ctx context.Context, responses ...*vtgatepb.VStreamResponse) func() (*vtgatepb.VStreamResponse, error) {
	i := 0
	return func() (*vtgatepb.VStreamResponse, error) {
		if i < len(responses) {
			i++
			return responses[i-1], nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
}

func shTimings(now time.Time) schemaHistoryTimings {
	return schemaHistoryTimings{grace: 2 * time.Minute, liveWait: 50 * time.Millisecond, settle: 20 * time.Millisecond, now: func() time.Time { return now }}
}

func TestScanSchemaHistoryStream(t *testing.T) {
	longAfter := time.Unix(shDDLTime, 0).Add(time.Hour)
	justAfter := time.Unix(shDDLTime, 0).Add(10 * time.Second)
	for _, tc := range []struct {
		name      string
		responses []*vtgatepb.VStreamResponse
		now       time.Time
		want      SchemaHistoryVerdict
		wantDDL   string
		wantErr   error
	}{
		{
			name: "DDL followed by VERSION: tracked",
			responses: []*vtgatepb.VStreamResponse{
				shResp(shVGTID("-", "MySQL56/a:1-11"), shRow(shDDLTime-5)),
				shResp(shVGTID("-", "MySQL56/a:1-12"), shDDL(shDDLTime)),
				shResp(shVGTID("-", "MySQL56/a:1-13"), shVersion(shDDLTime)),
				shResp(shVGTID("-", shStop)),
			},
			now: longAfter, want: SchemaHistoryTracked, wantDDL: shRename,
		},
		{
			name: "tracker delay: rows and other events between the DDL and its VERSION",
			responses: []*vtgatepb.VStreamResponse{
				shResp(shVGTID("-", "MySQL56/a:1-12"), shDDL(shDDLTime)),
				shResp(shVGTID("-", "MySQL56/a:1-13"), shRow(shDDLTime), shRow(shDDLTime+1)),
				shResp(shVGTID("-", "MySQL56/a:1-14"), shVersion(shDDLTime+3)),
				shResp(shVGTID("-", shStop)),
			},
			now: longAfter, want: SchemaHistoryTracked, wantDDL: shRename,
		},
		{
			name: "DDL with no VERSION before events written later than the grace: untracked",
			responses: []*vtgatepb.VStreamResponse{
				shResp(shVGTID("-", "MySQL56/a:1-12"), shDDL(shDDLTime)),
				shResp(shVGTID("-", "MySQL56/a:1-13"), shRow(shDDLTime+10)),
				shResp(shVGTID("-", "MySQL56/a:1-14"), shRow(shDDLTime+121)),
				shResp(shVGTID("-", "MySQL56/a:1-15"), shVersion(shDDLTime+200)),
			},
			now: longAfter, want: SchemaHistoryUntracked, wantDDL: shRename,
		},
		{
			name: "a VERSION before the table's DDL (another table) does not count",
			responses: []*vtgatepb.VStreamResponse{
				shResp(shVGTID("-", "MySQL56/a:1-11"), shVersion(shDDLTime-100)),
				shResp(shVGTID("-", "MySQL56/a:1-12"), shDDL(shDDLTime)),
				shResp(shVGTID("-", "MySQL56/a:1-13"), shRow(shDDLTime+500)),
			},
			now: longAfter, want: SchemaHistoryUntracked, wantDDL: shRename,
		},
		{
			name: "no DDL before the stream passes the stop position",
			responses: []*vtgatepb.VStreamResponse{
				shResp(shVGTID("-", "MySQL56/a:1-15"), shRow(shDDLTime)),
				shResp(shVGTID("-", shStop), shRow(shDDLTime)),
				shResp(shVGTID("-", "MySQL56/a:1-21"), shRow(shDDLTime)),
			},
			now: longAfter, want: SchemaHistoryNoDDL,
		},
		{
			name: "no DDL, idle at the stop position: settles",
			responses: []*vtgatepb.VStreamResponse{
				shResp(shVGTID("-", shStop), shRow(shDDLTime)),
			},
			now: longAfter, want: SchemaHistoryNoDDL,
		},
		{
			name: "DDL at the stop position, VERSION arrives while waiting",
			responses: []*vtgatepb.VStreamResponse{
				shResp(shVGTID("-", shStop), shDDL(shDDLTime)),
				shResp(shVGTID("-", "MySQL56/a:1-21"), shVersion(shDDLTime+1)),
			},
			now: justAfter, want: SchemaHistoryTracked, wantDDL: shRename,
		},
		{
			name: "old DDL at the stop position and no VERSION: untracked",
			responses: []*vtgatepb.VStreamResponse{
				shResp(shVGTID("-", shStop), shDDL(shDDLTime)),
			},
			now: longAfter, want: SchemaHistoryUntracked, wantDDL: shRename,
		},
		{
			name: "recent DDL at the stop position and no VERSION yet: inconclusive",
			responses: []*vtgatepb.VStreamResponse{
				shResp(shVGTID("-", shStop), shDDL(shDDLTime)),
			},
			now: justAfter, wantErr: ErrSchemaHistoryInconclusive, wantDDL: shRename,
		},
		{
			name: "another shard's positions are ignored",
			responses: []*vtgatepb.VStreamResponse{
				shResp(shVGTID("80-", shStop), shRow(shDDLTime)),
				shResp(shVGTID("80-", "MySQL56/a:1-21"), shRow(shDDLTime)),
				shResp(shVGTID("-", "MySQL56/a:1-12"), shDDL(shDDLTime)),
				shResp(shVGTID("-", "MySQL56/a:1-13"), shVersion(shDDLTime)),
				shResp(shVGTID("-", shStop), shRow(shDDLTime+5)),
				shResp(shVGTID("-", "MySQL56/a:1-21")),
			},
			now: longAfter, want: SchemaHistoryTracked, wantDDL: shRename,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			got, err := scanSchemaHistoryStream(ctx, shRecv(ctx, tc.responses...), shKeyspace, "-", shStop, shTimings(tc.now))
			if tc.wantErr != nil {
				assert.ErrorIs(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.want, got.Verdict)
			}
			assert.Equal(t, tc.wantDDL, got.DDL)
		})
	}
}

func TestScanSchemaHistoryStreamReturnsStreamErrors(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("vttablet: boom")
	recv := func() (*vtgatepb.VStreamResponse, error) { return nil, boom }
	_, err := scanSchemaHistoryStream(ctx, recv, shKeyspace, "-", shStop, shTimings(time.Now()))
	assert.ErrorIs(t, err, boom)
}

// ScanSchemaHistory asks for the current position first, then streams from
// the saved binlog position (never from a copy-phase LastKnownPk) with every
// column of the table.
func TestScanSchemaHistoryRequests(t *testing.T) {
	var requests []*vtgatepb.VStreamRequest
	mock := &vstreamConnectionMock{vstreamFn: func(ctx context.Context, in *vtgatepb.VStreamRequest, opts ...grpc.CallOption) (vtgateservicepb.Vitess_VStreamClient, error) {
		requests = append(requests, in)
		if in.Vgtid.ShardGtids[0].Gtid == "current" {
			return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{shResp(shVGTID("-80", shStop))}}, nil
		}
		return &vstreamClientMock{responses: []*vtgatepb.VStreamResponse{
			shResp(shVGTID("-80", "MySQL56/a:1-12"), shDDL(shDDLTime)),
			shResp(shVGTID("-80", "MySQL56/a:1-13"), shVersion(shDDLTime)),
			shResp(shVGTID("-80", shStop)),
			shResp(shVGTID("-80", "MySQL56/a:1-21")),
		}}, nil
	}}
	client := connectClient{vstreamClientFn: func(ctx context.Context, ps PlanetScaleSource) (vstreamClient, error) { return mock, nil }}
	logger := &dbLogger{}
	tc := &psdbconnect.TableCursor{Keyspace: shKeyspace, Shard: "-80", Position: shStart, LastKnownPk: testLastKnownPK("42")}
	got, err := client.ScanSchemaHistory(context.Background(), logger, PlanetScaleSource{Database: shKeyspace}, "t", tc)
	assert.NoError(t, err)
	assert.Equal(t, SchemaHistoryTracked, got.Verdict)
	if assert.Len(t, requests, 2) {
		scan := requests[1]
		assert.Equal(t, shStart, scan.Vgtid.ShardGtids[0].Gtid)
		assert.Empty(t, scan.Vgtid.ShardGtids[0].TablePKs)
		assert.Equal(t, "SELECT * FROM `t`", scan.Filter.Rules[0].Filter)
	}

	// Nothing to scan when the saved position is the current one.
	requests = nil
	tc = &psdbconnect.TableCursor{Keyspace: shKeyspace, Shard: "-80", Position: shStop}
	got, err = client.ScanSchemaHistory(context.Background(), logger, PlanetScaleSource{Database: shKeyspace}, "t", tc)
	assert.NoError(t, err)
	assert.Equal(t, SchemaHistoryNoDDL, got.Verdict)
	assert.Len(t, requests, 1)
}

func TestConnectClientScansSchemaHistory(t *testing.T) {
	var mysql MysqlClient
	_, ok := NewConnectClient(&mysql).(SchemaHistoryScanner)
	assert.True(t, ok, "the handler finds the scanner by type assertion on the ConnectClient")
}

// Review finding 2: a VERSION event only shows that the table's schema change
// was recorded if it follows that change within the grace. A VERSION much
// later (the tracker was turned on afterwards and another table changed) must
// not make an untracked change look tracked; nor may one VERSION cover two
// schema changes on the table.
func TestScanSchemaHistoryStreamVersionMustFollowEachDDLWithinGrace(t *testing.T) {
	longAfter := time.Unix(shDDLTime, 0).Add(2 * time.Hour)
	for _, tc := range []struct {
		name      string
		responses []*vtgatepb.VStreamResponse
	}{
		{
			name: "VERSION an hour after the DDL, nothing in between",
			responses: []*vtgatepb.VStreamResponse{
				shResp(shVGTID("-", "MySQL56/a:1-12"), shDDL(shDDLTime)),
				shResp(shVGTID("-", "MySQL56/a:1-13"), shVersion(shDDLTime+3600)),
				shResp(shVGTID("-", shStop)),
			},
		},
		{
			name: "two DDLs, one VERSION",
			responses: []*vtgatepb.VStreamResponse{
				shResp(shVGTID("-", "MySQL56/a:1-12"), shDDL(shDDLTime)),
				shResp(shVGTID("-", "MySQL56/a:1-13"), shDDL(shDDLTime+30)),
				shResp(shVGTID("-", "MySQL56/a:1-14"), shVersion(shDDLTime+31)),
				shResp(shVGTID("-", shStop)),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			got, err := scanSchemaHistoryStream(ctx, shRecv(ctx, tc.responses...), shKeyspace, "-", shStop, shTimings(longAfter))
			assert.NoError(t, err)
			assert.Equal(t, SchemaHistoryUntracked, got.Verdict)
		})
	}
}

// Review finding 5: a scan that runs out of time before it reaches the current
// position (a large backlog) is not "inconclusive, try again": the next scan
// starts from the same cursor and runs out of time again.
func TestScanSchemaHistoryStreamTimeoutIsNotInconclusive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	responses := []*vtgatepb.VStreamResponse{shResp(shVGTID("-", "MySQL56/a:1-11"), shRow(shDDLTime))}
	_, err := scanSchemaHistoryStream(ctx, shRecv(ctx, responses...), shKeyspace, "-", shStop, shTimings(time.Unix(shDDLTime, 0)))
	assert.ErrorIs(t, err, ErrSchemaHistoryScanTimeout)
	assert.NotErrorIs(t, err, ErrSchemaHistoryInconclusive)
}

func TestScanSchemaHistoryStreamEveryDDLWithItsVersionIsTracked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	responses := []*vtgatepb.VStreamResponse{
		shResp(shVGTID("-", "MySQL56/a:1-12"), shDDL(shDDLTime)),
		shResp(shVGTID("-", "MySQL56/a:1-13"), shVersion(shDDLTime+1)),
		shResp(shVGTID("-", "MySQL56/a:1-14"), shDDL(shDDLTime+600)),
		shResp(shVGTID("-", "MySQL56/a:1-15"), shDDL(shDDLTime+601)),
		shResp(shVGTID("-", "MySQL56/a:1-16"), shVersion(shDDLTime+601), shVersion(shDDLTime+602)),
		shResp(shVGTID("-", shStop)),
	}
	got, err := scanSchemaHistoryStream(ctx, shRecv(ctx, responses...), shKeyspace, "-", shStop, shTimings(time.Unix(shDDLTime, 0).Add(time.Hour)))
	assert.NoError(t, err)
	assert.Equal(t, SchemaHistoryTracked, got.Verdict)
}

// Second review, finding 1: a schema change recorded by the tracker says
// nothing about a later one. The scan reads up to the current position and
// every DDL event needs its own VERSION event, even after an earlier DDL was
// tracked (the tracker turned off in between).
func TestScanSchemaHistoryStreamLaterUntrackedDDLAfterTrackedOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	responses := []*vtgatepb.VStreamResponse{
		shResp(shVGTID("-", "MySQL56/a:1-12"), shDDL(shDDLTime)),
		shResp(shVGTID("-", "MySQL56/a:1-13"), shVersion(shDDLTime+1)),
		shResp(shVGTID("-", "MySQL56/a:1-14"), shRow(shDDLTime+600)),
		shResp(shVGTID("-", "MySQL56/a:1-15"), shDDL(shDDLTime+3600)),
		shResp(shVGTID("-", "MySQL56/a:1-16"), shRow(shDDLTime+3700)),
		shResp(shVGTID("-", shStop), shRow(shDDLTime+3800)),
		shResp(shVGTID("-", "MySQL56/a:1-21")),
	}
	got, err := scanSchemaHistoryStream(ctx, shRecv(ctx, responses...), shKeyspace, "-", shStop, shTimings(time.Unix(shDDLTime, 0).Add(2*time.Hour)))
	assert.NoError(t, err)
	assert.Equal(t, SchemaHistoryUntracked, got.Verdict)

	// The same later DDL right at the current position, with no VERSION event
	// for longer than the grace: untracked once the live wait ends.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	responses = []*vtgatepb.VStreamResponse{
		shResp(shVGTID("-", "MySQL56/a:1-12"), shDDL(shDDLTime)),
		shResp(shVGTID("-", "MySQL56/a:1-13"), shVersion(shDDLTime+1)),
		shResp(shVGTID("-", shStop), shDDL(shDDLTime+3600)),
	}
	got, err = scanSchemaHistoryStream(ctx2, shRecv(ctx2, responses...), shKeyspace, "-", shStop, shTimings(time.Unix(shDDLTime, 0).Add(2*time.Hour)))
	assert.NoError(t, err)
	assert.Equal(t, SchemaHistoryUntracked, got.Verdict)
}

// Second review, nit: a DDL event without a timestamp must resolve. Dated
// with the event before it, the grace applies as usual; with no dated event
// at all, the scan ends untracked (Task) instead of inconclusive forever.
func TestScanSchemaHistoryStreamUndatedDDL(t *testing.T) {
	for _, tc := range []struct {
		name      string
		responses []*vtgatepb.VStreamResponse
		now       time.Time
		want      SchemaHistoryVerdict
	}{
		{
			name: "dated by the event before it, no VERSION within the grace: untracked",
			responses: []*vtgatepb.VStreamResponse{
				shResp(shVGTID("-", "MySQL56/a:1-11"), shRow(shDDLTime)),
				shResp(shVGTID("-", "MySQL56/a:1-12"), shDDL(0)),
				shResp(shVGTID("-", "MySQL56/a:1-13"), shRow(shDDLTime+500)),
			},
			now:  time.Unix(shDDLTime, 0).Add(time.Hour),
			want: SchemaHistoryUntracked,
		},
		{
			name: "dated by the event before it, VERSION right after: tracked",
			responses: []*vtgatepb.VStreamResponse{
				shResp(shVGTID("-", "MySQL56/a:1-11"), shRow(shDDLTime)),
				shResp(shVGTID("-", "MySQL56/a:1-12"), shDDL(0)),
				shResp(shVGTID("-", "MySQL56/a:1-13"), shVersion(shDDLTime+1)),
				shResp(shVGTID("-", shStop)),
			},
			now:  time.Unix(shDDLTime, 0).Add(time.Hour),
			want: SchemaHistoryTracked,
		},
		{
			name: "no dated event at all, idle at the current position: untracked, not inconclusive",
			responses: []*vtgatepb.VStreamResponse{
				shResp(shVGTID("-", shStop), shDDL(0)),
			},
			now:  time.Unix(shDDLTime, 0),
			want: SchemaHistoryUntracked,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			got, err := scanSchemaHistoryStream(ctx, shRecv(ctx, tc.responses...), shKeyspace, "-", shStop, shTimings(tc.now))
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got.Verdict)
		})
	}
}

// Second review, finding 5: when reading the current position fails (for
// example vtgate finds no tablet during a reparent), the scan never streamed
// from the saved position; the error is ErrSchemaHistoryPositionCheck and
// keeps its gRPC status.
func TestScanSchemaHistoryPositionCheckFailure(t *testing.T) {
	notFound := status.Error(codes.Unknown, "failed to find a PRIMARY tablet for VStream in ks/-80 within the planetscale_operator_default cell(s): context has expired")
	var requests int
	mock := &vstreamConnectionMock{vstreamFn: func(ctx context.Context, in *vtgatepb.VStreamRequest, opts ...grpc.CallOption) (vtgateservicepb.Vitess_VStreamClient, error) {
		requests++
		return nil, notFound
	}}
	client := connectClient{vstreamClientFn: func(ctx context.Context, ps PlanetScaleSource) (vstreamClient, error) { return mock, nil }}
	tc := &psdbconnect.TableCursor{Keyspace: shKeyspace, Shard: "-80", Position: shStart}
	_, err := client.ScanSchemaHistory(context.Background(), &dbLogger{}, PlanetScaleSource{Database: shKeyspace}, "t", tc)
	assert.ErrorIs(t, err, ErrSchemaHistoryPositionCheck)
	assert.Equal(t, codes.Unknown, status.Code(err))
	assert.Equal(t, 1, requests, "no scan stream after the failed position check")
}
