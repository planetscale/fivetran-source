package lib

import (
	"context"
	"errors"
	"testing"
	"time"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
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
			},
			now: longAfter, want: SchemaHistoryTracked, wantDDL: shRename,
		},
		{
			name: "tracker delay: rows and other events between the DDL and its VERSION",
			responses: []*vtgatepb.VStreamResponse{
				shResp(shVGTID("-", "MySQL56/a:1-12"), shDDL(shDDLTime)),
				shResp(shVGTID("-", "MySQL56/a:1-13"), shRow(shDDLTime), shRow(shDDLTime+1)),
				shResp(shVGTID("-", "MySQL56/a:1-14"), shVersion(shDDLTime+3)),
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
