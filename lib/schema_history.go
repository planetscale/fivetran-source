package lib

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

// SchemaHistoryVerdict says whether binlog events between a saved cursor and
// the current position can be decoded with the schema they were written with.
type SchemaHistoryVerdict int

const (
	// SchemaHistoryNoDDL: no schema change on the table between the cursor
	// and the current position, so every event there has the current layout.
	SchemaHistoryNoDDL SchemaHistoryVerdict = iota + 1
	// SchemaHistoryTracked: a schema change on the table was followed by a
	// VERSION event, so vttablet's schema tracker is on and the historian
	// decodes older events with the schema they were written with.
	SchemaHistoryTracked
	// SchemaHistoryUntracked: a schema change on the table with no VERSION
	// event after it, so older events are decoded by position against the
	// current schema.
	SchemaHistoryUntracked
)

func (v SchemaHistoryVerdict) String() string {
	switch v {
	case SchemaHistoryNoDDL:
		return "no schema change since the cursor"
	case SchemaHistoryTracked:
		return "schema change tracked by the schema historian"
	case SchemaHistoryUntracked:
		return "schema change not tracked (no schema version recorded)"
	default:
		return "unknown"
	}
}

// SchemaHistory is the result of ScanSchemaHistory for one shard.
type SchemaHistory struct {
	Verdict SchemaHistoryVerdict
	// DDL is the statement of the first schema change seen on the table (for
	// Online DDL, the cut-over RENAME), or "".
	DDL string
}

// SchemaHistoryScanner is implemented by a ConnectClient that can tell, for a
// table and a saved cursor, whether a schema change since the cursor was
// recorded by vttablet's schema tracker (--track-schema-versions).
type SchemaHistoryScanner interface {
	ScanSchemaHistory(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, tableName string, tc *psdbconnect.TableCursor) (SchemaHistory, error)
}

var _ SchemaHistoryScanner = connectClient{}

var (
	// ErrSchemaHistoryUnavailable: the client cannot scan the schema history.
	ErrSchemaHistoryUnavailable = errors.New("schema history scan is not available")
	// ErrSchemaHistoryInconclusive: the scan ended before it could tell, for
	// example right after a schema change, before the tracker recorded it.
	ErrSchemaHistoryInconclusive = errors.New("could not yet tell whether the schema change was recorded by the schema tracker")
)

const (
	// schemaVersionGrace is how much binlog time may pass between a schema
	// change and the VERSION event of the tracker's insert into
	// _vt.schema_version. The tracker inserts as soon as its own stream
	// sees the DDL, normally well under a second later.
	schemaVersionGrace = 2 * time.Minute
	// schemaVersionLiveWait is how long the scan waits at the current
	// position for a VERSION event after a schema change it just saw.
	schemaVersionLiveWait = 30 * time.Second
	// schemaHistorySettle is how long the scan keeps reading at the current
	// position for a schema change at exactly that position.
	schemaHistorySettle = 5 * time.Second
	// schemaHistoryScanTimeout bounds the whole scan of one shard.
	schemaHistoryScanTimeout = 15 * time.Minute
)

// ScanSchemaHistory streams the table's binlog events from tc's position to
// the current position, emitting nothing, and reports whether a schema change
// on the table in that range was followed by a VERSION event.
//
// vttablet only sends DDL events for statements that name the table
// (mustSendDDL); for Online DDL that is the cut-over RENAME. It sends VERSION
// events, whatever the filter, for every insert into _vt.schema_version, which
// only the schema tracker makes (one per schema change, on every shard's
// primary). A schema change followed by a VERSION event therefore means the
// historian has the table's schema from before the change and decodes older
// row events with it; without one, older row events are decoded by position
// against the current schema.
//
// A copy-phase cursor (LastKnownPk) is scanned from its binlog position: the
// copy itself reads the current table.
func (p connectClient) ScanSchemaHistory(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, tableName string, tc *psdbconnect.TableCursor) (SchemaHistory, error) {
	if tc == nil || tc.Position == "" {
		return SchemaHistory{Verdict: SchemaHistoryNoDDL}, nil
	}
	tabletType := psdbconnect.TabletType_primary
	if ps.UseReplica {
		tabletType = psdbconnect.TabletType_replica
	}
	preamble := fmt.Sprintf("[%v:%v shard:%v tabletType:%s] ", ps.Database, tableName, tc.Shard, tabletType)

	stop, err := p.getLatestCursorPosition(ctx, tc.Shard, tc.Keyspace, tableName, ps, tabletType)
	if err != nil {
		return SchemaHistory{}, errors.Wrap(err, "Unable to get latest cursor position")
	}
	if stop == tc.Position {
		return SchemaHistory{Verdict: SchemaHistoryNoDDL}, nil
	}

	ctx, cancel := context.WithTimeout(ctx, schemaHistoryScanTimeout)
	defer cancel()
	client, closeClient, err := p.newVStreamClient(ctx, ps)
	if err != nil {
		return SchemaHistory{}, err
	}
	defer closeClient()

	logger.Info(fmt.Sprintf("%sscanning binlog events since the saved cursor for schema changes and schema versions", preamble))
	start := &psdbconnect.TableCursor{Shard: tc.Shard, Keyspace: tc.Keyspace, Position: tc.Position}
	stream, err := client.VStream(ctx, buildVStreamRequest(tableName, nil, start, tabletType))
	if err != nil {
		return SchemaHistory{}, err
	}
	history, err := scanSchemaHistoryStream(ctx, stream.Recv, tc.Keyspace, tc.Shard, stop, schemaHistoryTimings{
		grace: schemaVersionGrace, liveWait: schemaVersionLiveWait, settle: schemaHistorySettle, now: time.Now,
	})
	if err != nil {
		return history, err
	}
	logger.Info(fmt.Sprintf("%sschema history: %s", preamble, history.Verdict))
	if history.DDL != "" {
		logger.Info(fmt.Sprintf("%sschema change seen: %q", preamble, history.DDL))
	}
	return history, nil
}

type schemaHistoryTimings struct {
	grace, liveWait, settle time.Duration
	now                     func() time.Time
}

// scanSchemaHistoryStream reads VStream responses (recv) for one shard until
// it can tell the verdict:
//
//   - a VERSION event after a DDL event: SchemaHistoryTracked;
//   - an event written grace (binlog time) or more after a DDL event, with no
//     VERSION event in between: SchemaHistoryUntracked;
//   - the stream passes stop (the position when the scan started) with no DDL
//     event, or stays at stop for settle: SchemaHistoryNoDDL;
//   - a DDL event and then nothing for liveWait at stop: SchemaHistoryUntracked
//     if the DDL is older than grace, else ErrSchemaHistoryInconclusive (the
//     next sync looks again).
func scanSchemaHistoryStream(ctx context.Context, recv func() (*vtgatepb.VStreamResponse, error), keyspace, shard, stop string, t schemaHistoryTimings) (SchemaHistory, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type received struct {
		res *vtgatepb.VStreamResponse
		err error
	}
	ch := make(chan received)
	go func() {
		for {
			res, err := recv()
			select {
			case ch <- received{res, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	var (
		history     SchemaHistory
		pending     bool  // a DDL event with no VERSION event after it yet
		ddlTime     int64 // binlog timestamp (seconds) of the pending DDL
		reachedStop bool
		timer       *time.Timer
		timerC      <-chan time.Time
		timerForDDL bool
	)
	setTimer := func(d time.Duration, forDDL bool) {
		if timer != nil {
			timer.Stop()
		}
		timer = time.NewTimer(d)
		timerC = timer.C
		timerForDDL = forDDL
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	untrackedOrInconclusive := func() (SchemaHistory, error) {
		if ddlTime > 0 && t.now().Sub(time.Unix(ddlTime, 0)) >= t.grace {
			history.Verdict = SchemaHistoryUntracked
			return history, nil
		}
		return history, ErrSchemaHistoryInconclusive
	}

	for {
		select {
		case <-ctx.Done():
			return history, errors.Wrap(ErrSchemaHistoryInconclusive, ctx.Err().Error())
		case <-timerC:
			if !pending {
				history.Verdict = SchemaHistoryNoDDL
				return history, nil
			}
			return untrackedOrInconclusive()
		case r := <-ch:
			if r.err != nil {
				return history, r.err
			}
			passedStop := false
			for _, event := range r.res.GetEvents() {
				switch event.GetType() {
				case binlogdatapb.VEventType_VGTID:
					if pos := vgtidPositionForShard([]*binlogdatapb.VEvent{event}, keyspace, shard); pos != "" {
						if pos == stop {
							reachedStop = true
						} else if reachedStop {
							passedStop = true
						}
					}
				case binlogdatapb.VEventType_DDL:
					if !pending {
						pending = true
						ddlTime = event.GetTimestamp()
					}
					if history.DDL == "" {
						history.DDL = event.GetStatement()
					}
				case binlogdatapb.VEventType_VERSION:
					if pending {
						history.Verdict = SchemaHistoryTracked
						return history, nil
					}
				}
				if pending && ddlTime > 0 && event.GetTimestamp()-ddlTime >= int64(t.grace/time.Second) {
					history.Verdict = SchemaHistoryUntracked
					return history, nil
				}
			}
			if passedStop && !pending {
				history.Verdict = SchemaHistoryNoDDL
				return history, nil
			}
			if reachedStop {
				switch {
				case pending && (timerC == nil || !timerForDDL):
					setTimer(t.liveWait, true)
				case !pending && timerC == nil:
					setTimer(t.settle, false)
				}
			}
		}
	}
}
