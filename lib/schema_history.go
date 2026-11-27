package lib

import (
	"context"
	"fmt"
	"slices"
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
	// SchemaHistoryTracked: every schema change on the table between the
	// cursor and the current position was followed by its own VERSION event,
	// so vttablet's schema tracker recorded each of them and the historian
	// decodes older events with the schema they were written with.
	SchemaHistoryTracked
	// SchemaHistoryUntracked: a schema change on the table with no VERSION
	// event after it (any one of them, if there are several), so older events
	// are decoded by position against the current schema.
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
	// Progress is where the scan got to, with everything it learned on the
	// way, also when it returns an error (for example
	// ErrSchemaHistoryScanTimeout); nil if it never got past its start. The
	// caller saves it on the shard's cursor so that the next scan of the same
	// cursor resumes from there (see SchemaScanProgress).
	Progress *SchemaScanProgress
}

// SchemaScanProgress is how far a schema history scan of one shard got from
// the shard's saved cursor, saved on that cursor (SerializedCursor.SchemaScan)
// so that a scan cut short by schemaHistoryScanTimeout (a binlog backlog
// larger than one scan can read) resumes on the next sync instead of starting
// over, and a table with a large backlog gets there over a few syncs instead
// of a re-sync Task. A new cursor (any progress of the shard's read, or a
// reset) has none, so the progress always belongs to the cursor it is saved
// on. Older connector versions ignore it.
type SchemaScanProgress struct {
	// Position is the binlog position (a VGTID of the shard) the scan read up
	// to: every event up to it was looked at.
	Position string `json:"position"`
	// PendingDDLs are the binlog timestamps (seconds) of the table's schema
	// changes seen so far that have no VERSION event after them yet.
	PendingDDLs []int64 `json:"pending_ddls,omitempty"`
	// Tracked: a schema change seen so far was followed by its VERSION event.
	Tracked bool `json:"tracked,omitempty"`
	// Untracked: the scan already found a schema change without a VERSION
	// event (SchemaHistoryUntracked); a resumed scan reports it at once.
	Untracked bool `json:"untracked,omitempty"`
	// DDL is the first schema change seen on the table, or "".
	DDL string `json:"ddl,omitempty"`
	// LastTimestamp is the latest event timestamp seen (dates an undated DDL).
	LastTimestamp int64 `json:"last_timestamp,omitempty"`
}

// SchemaHistoryScanner is implemented by a ConnectClient that can tell, for a
// table and a saved cursor, whether a schema change since the cursor was
// recorded by vttablet's schema tracker (--track-schema-versions).
//
// resume is the progress of an earlier scan of the same cursor (or nil): the
// scan continues from resume.Position with what that scan learned.
type SchemaHistoryScanner interface {
	ScanSchemaHistory(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, tableName string, tc *psdbconnect.TableCursor, resume *SchemaScanProgress) (SchemaHistory, error)
}

var _ SchemaHistoryScanner = connectClient{}

var (
	// ErrSchemaHistoryUnavailable: the client cannot scan the schema history.
	ErrSchemaHistoryUnavailable = errors.New("schema history scan is not available")
	// ErrSchemaHistoryInconclusive: the scan ended before it could tell, for
	// example right after a schema change, before the tracker recorded it.
	ErrSchemaHistoryInconclusive = errors.New("could not yet tell whether the schema change was recorded by the schema tracker")
	// ErrSchemaHistoryScanTimeout: the scan did not reach the current position
	// within schemaHistoryScanTimeout (a large binlog backlog since the saved
	// cursor). SchemaHistory.Progress says how far it got: the caller saves it
	// so the next sync resumes there, and only raises the re-sync Task when
	// the scan got nowhere.
	ErrSchemaHistoryScanTimeout = errors.New("the schema history scan did not reach the current binlog position in time")
	// ErrSchemaHistoryPositionCheck: the scan could not read the shard's
	// current binlog position, so it never streamed from the saved cursor. A
	// "no tablet found" error here says nothing about the saved position
	// (unlike the same error from the scan's own stream): the caller must not
	// treat it as suspected binlog expiration.
	ErrSchemaHistoryPositionCheck = errors.New("could not read the current binlog position for the schema history scan")
)

// Variables, not constants, so tests can shorten them (SetSchemaHistoryTimings).
var (
	// schemaVersionGrace is how much binlog time may pass between a schema
	// change and the VERSION event of the tracker's insert into
	// _vt.schema_version. The tracker inserts as soon as its own stream
	// sees the DDL, normally well under a second later.
	schemaVersionGrace = 2 * time.Minute
	// schemaVersionLiveWait is how long the scan waits at the current
	// position for a VERSION event after a schema change it just saw.
	schemaVersionLiveWait = 30 * time.Second
)

const (
	// schemaHistorySettle is how long the scan keeps reading at the current
	// position for a schema change at exactly that position.
	schemaHistorySettle = 5 * time.Second
	// schemaHistoryScanTimeout bounds the whole scan of one shard.
	schemaHistoryScanTimeout = 15 * time.Minute
)

// SetSchemaHistoryTimings sets the grace a VERSION event has after a schema
// change and how long the scan waits for one at the current position, and
// returns a func that restores the previous values. For tests only (e.g. the
// integration tests, which would otherwise wait out the 2-minute grace).
func SetSchemaHistoryTimings(grace, liveWait time.Duration) (restore func()) {
	prevGrace, prevLiveWait := schemaVersionGrace, schemaVersionLiveWait
	schemaVersionGrace, schemaVersionLiveWait = grace, liveWait
	return func() { schemaVersionGrace, schemaVersionLiveWait = prevGrace, prevLiveWait }
}

// ScanSchemaHistory streams the table's binlog events from tc's position to
// the current position, emitting nothing, and reports whether each schema
// change on the table in that range was followed by a VERSION event within
// schemaVersionGrace (see scanSchemaHistoryStream).
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
func (p connectClient) ScanSchemaHistory(ctx context.Context, logger DatabaseLogger, ps PlanetScaleSource, tableName string, tc *psdbconnect.TableCursor, resume *SchemaScanProgress) (SchemaHistory, error) {
	if tc == nil || tc.Position == "" {
		return SchemaHistory{Verdict: SchemaHistoryNoDDL}, nil
	}
	from := tc.Position
	if resume != nil && resume.Position != "" {
		if resume.Untracked {
			return SchemaHistory{Verdict: SchemaHistoryUntracked, DDL: resume.DDL, Progress: resume}, nil
		}
		from = resume.Position
	} else {
		resume = nil
	}
	tabletType := psdbconnect.TabletType_primary
	if ps.UseReplica {
		tabletType = psdbconnect.TabletType_replica
	}
	preamble := fmt.Sprintf("[%v:%v shard:%v tabletType:%s] ", ps.Database, tableName, tc.Shard, tabletType)

	stop, err := p.getLatestCursorPosition(ctx, tc.Shard, tc.Keyspace, tableName, ps, tabletType)
	if err != nil {
		// Both are kept in the chain: errors.Is for the sentinel, and the
		// gRPC status of err for the caller's connection-wide check.
		return SchemaHistory{}, fmt.Errorf("%w: %w", ErrSchemaHistoryPositionCheck, err)
	}
	if stop == from {
		if resume != nil {
			// Nothing written since the earlier scan stopped: its findings
			// are the verdict (a schema change still without a VERSION event
			// gets the same live wait as at the end of a scan).
			return resumedVerdict(resume, schemaVersionGrace, time.Now)
		}
		return SchemaHistory{Verdict: SchemaHistoryNoDDL}, nil
	}

	ctx, cancel := context.WithTimeout(ctx, schemaHistoryScanTimeout)
	defer cancel()
	client, closeClient, err := p.newVStreamClient(ctx, ps)
	if err != nil {
		return SchemaHistory{}, err
	}
	defer closeClient()

	if resume != nil {
		logger.Info(fmt.Sprintf("%sresuming the scan of binlog events since the saved cursor from where the previous sync stopped", preamble))
	} else {
		logger.Info(fmt.Sprintf("%sscanning binlog events since the saved cursor for schema changes and schema versions", preamble))
	}
	start := &psdbconnect.TableCursor{Shard: tc.Shard, Keyspace: tc.Keyspace, Position: from}
	stream, err := client.VStream(ctx, buildVStreamRequest(tableName, nil, start, tabletType))
	if err != nil {
		return SchemaHistory{}, err
	}
	history, err := scanSchemaHistoryStream(ctx, stream.Recv, tc.Keyspace, tc.Shard, stop, schemaHistoryTimings{
		grace: schemaVersionGrace, liveWait: schemaVersionLiveWait, settle: schemaHistorySettle, now: time.Now,
	}, resume)
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
// it can tell the verdict. Every DDL event on the table must be followed by
// its own VERSION event within grace (binlog time); a VERSION event resolves
// the oldest DDL still waiting for one. A VERSION event written later than
// that (for example after the tracker was turned on, for another table's
// schema change) does not show that the table's change was recorded. The scan
// always reads up to stop: a schema change recorded by the tracker says
// nothing about a later one (the tracker may have been turned off since).
//
//   - a VERSION event, or any other event, written grace or more after a DDL
//     event that has no VERSION event yet: SchemaHistoryUntracked (at once);
//   - the stream passes stop (the position when the scan started), or stays
//     at stop for settle, with every DDL event followed by its VERSION event:
//     SchemaHistoryTracked, or SchemaHistoryNoDDL if there was no DDL event;
//   - a DDL event and then nothing for liveWait at stop: SchemaHistoryUntracked
//     if the DDL is older than grace, else ErrSchemaHistoryInconclusive (the
//     next sync looks again);
//   - ctx's deadline before any of these: ErrSchemaHistoryScanTimeout.
//
// A DDL event without a timestamp is dated with the latest event timestamp
// before it (or else the first one after it), so the grace never makes it
// lenient; one still undated when the scan ends counts as untracked, so the
// table gets the Task instead of an inconclusive scan on every sync.
// resume (or nil) carries what an earlier scan of the same cursor learned,
// up to the position the stream starts from. Every return sets
// history.Progress to where the scan got (resume if it got nowhere new).
func scanSchemaHistoryStream(ctx context.Context, recv func() (*vtgatepb.VStreamResponse, error), keyspace, shard, stop string, t schemaHistoryTimings, resume *SchemaScanProgress) (history SchemaHistory, err error) {
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
		// pendingDDLs holds the binlog timestamps (seconds) of the table's DDL
		// events that have no VERSION event after them yet, oldest first; 0
		// for a DDL event that has no timestamp and no event dated before it.
		pendingDDLs []int64
		// tracked: at least one DDL event was followed by its VERSION event.
		tracked bool
		// lastTimestamp is the latest non-zero event timestamp seen.
		lastTimestamp int64
		reachedStop   bool
		// position is the last VGTID of the shard the scan read up to.
		position    string
		timer       *time.Timer
		timerC      <-chan time.Time
		timerForDDL bool
	)
	if resume != nil {
		pendingDDLs = append(pendingDDLs, resume.PendingDDLs...)
		tracked = resume.Tracked
		lastTimestamp = resume.LastTimestamp
		history.DDL = resume.DDL
		position = resume.Position
	}
	defer func() {
		if position == "" {
			return
		}
		history.Progress = &SchemaScanProgress{
			Position:      position,
			PendingDDLs:   slices.Clone(pendingDDLs),
			Tracked:       tracked,
			Untracked:     history.Verdict == SchemaHistoryUntracked,
			DDL:           history.DDL,
			LastTimestamp: lastTimestamp,
		}
	}()
	graceSeconds := int64(t.grace / time.Second)
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
		// An undated DDL can never be shown to be recent: raise the Task
		// rather than scan again on every sync.
		if ddlTime := pendingDDLs[0]; ddlTime == 0 || t.now().Sub(time.Unix(ddlTime, 0)) >= t.grace {
			history.Verdict = SchemaHistoryUntracked
			return history, nil
		}
		return history, ErrSchemaHistoryInconclusive
	}
	noPendingVerdict := func() SchemaHistory {
		if tracked {
			history.Verdict = SchemaHistoryTracked
		} else {
			history.Verdict = SchemaHistoryNoDDL
		}
		return history
	}
	ctxErr := func() error {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return errors.Wrap(ErrSchemaHistoryScanTimeout, ctx.Err().Error())
		}
		return errors.Wrap(ErrSchemaHistoryInconclusive, ctx.Err().Error())
	}

	for {
		select {
		case <-ctx.Done():
			return history, ctxErr()
		case <-timerC:
			if len(pendingDDLs) == 0 {
				return noPendingVerdict(), nil
			}
			return untrackedOrInconclusive()
		case r := <-ch:
			if r.err != nil {
				if ctx.Err() != nil {
					return history, ctxErr()
				}
				return history, r.err
			}
			passedStop := false
			for _, event := range r.res.GetEvents() {
				if ts := event.GetTimestamp(); ts > 0 {
					lastTimestamp = ts
					for i := range pendingDDLs {
						if pendingDDLs[i] == 0 {
							pendingDDLs[i] = ts
						}
					}
				}
				switch event.GetType() {
				case binlogdatapb.VEventType_VGTID:
					if pos := vgtidPositionForShard([]*binlogdatapb.VEvent{event}, keyspace, shard); pos != "" {
						position = pos
						if pos == stop {
							reachedStop = true
						} else if reachedStop {
							passedStop = true
						}
					}
				case binlogdatapb.VEventType_DDL:
					ddlTime := event.GetTimestamp()
					if ddlTime == 0 {
						ddlTime = lastTimestamp
					}
					pendingDDLs = append(pendingDDLs, ddlTime)
					if history.DDL == "" {
						history.DDL = event.GetStatement()
					}
				case binlogdatapb.VEventType_VERSION:
					if len(pendingDDLs) == 0 {
						break
					}
					versionTime := event.GetTimestamp()
					if versionTime == 0 {
						versionTime = lastTimestamp
					}
					if ddlTime := pendingDDLs[0]; ddlTime > 0 && versionTime-ddlTime >= graceSeconds {
						history.Verdict = SchemaHistoryUntracked
						return history, nil
					}
					pendingDDLs = pendingDDLs[1:]
					tracked = true
				}
				if len(pendingDDLs) > 0 && pendingDDLs[0] > 0 && event.GetTimestamp()-pendingDDLs[0] >= graceSeconds {
					history.Verdict = SchemaHistoryUntracked
					return history, nil
				}
			}
			pending := len(pendingDDLs) > 0
			if passedStop && !pending {
				return noPendingVerdict(), nil
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

// resumedVerdict is the verdict of a resumed scan that had nothing left to
// read: the earlier scan's findings, with a schema change still waiting for
// its VERSION event judged like at the end of a scan (untracked once it is
// older than grace, else inconclusive).
func resumedVerdict(resume *SchemaScanProgress, grace time.Duration, now func() time.Time) (SchemaHistory, error) {
	history := SchemaHistory{DDL: resume.DDL, Progress: resume}
	switch {
	case len(resume.PendingDDLs) > 0:
		if ddlTime := resume.PendingDDLs[0]; ddlTime == 0 || now().Sub(time.Unix(ddlTime, 0)) >= grace {
			history.Verdict = SchemaHistoryUntracked
			return history, nil
		}
		return history, ErrSchemaHistoryInconclusive
	case resume.Tracked:
		history.Verdict = SchemaHistoryTracked
	default:
		history.Verdict = SchemaHistoryNoDDL
	}
	return history, nil
}
