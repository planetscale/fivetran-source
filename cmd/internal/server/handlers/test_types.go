package handlers

import (
	"fmt"

	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk.v2"
	"github.com/planetscale/fivetran-source/lib"
	"vitess.io/vitess/go/sqltypes"
)

type testLogSender struct {
	sendError    error
	lastResponse *fivetransdk.UpdateResponse
}

func (l *testLogSender) Send(response *fivetransdk.UpdateResponse) error {
	l.lastResponse = response
	return l.sendError
}

type testLogger struct {
	truncateCalled bool
	truncateErr    error
	states         []lib.SyncState
	tasks          []string
	warnings       []string
	events         []string // order of alerts sent: "warning" / "task"
	binlogExpired  binlogExpirationSet
}

func (tl *testLogger) RecordBinlogExpiration(keyspace, table, shard string) {
	tl.binlogExpired.add(keyspace, table, shard)
}

func (tl *testLogger) FlushBinlogExpirationWarning() error {
	msg := tl.binlogExpired.warningMessage()
	tl.binlogExpired = binlogExpirationSet{}
	if msg == "" {
		return nil
	}
	return tl.SendWarningAlert(msg)
}

func (testLogger) Info(s string) error {
	// TODO implement me
	panic("implement me")
}

func (testLogger) Warning(s string) error {
	// TODO implement me
	panic("implement me")
}

func (tl *testLogger) SendWarningAlert(s string) error {
	tl.warnings = append(tl.warnings, s)
	tl.events = append(tl.events, "warning")
	return nil
}

func (tl *testLogger) SendTask(s string) error {
	tl.tasks = append(tl.tasks, s)
	tl.events = append(tl.events, "task")
	return nil
}

func (t *testLogger) Severe(s string) error {
	return nil
}

func (tl *testLogger) Log(level string, msg string) error {
	// TODO implement me
	panic("implement me")
}

func (tl *testLogger) Update(*lib.UpdatedRow, *fivetransdk.SchemaSelection, *fivetransdk.TableSelection) error {
	return fmt.Errorf("%v is not implemented", "Update")
}

func (tl *testLogger) Truncate(*fivetransdk.SchemaSelection, *fivetransdk.TableSelection) error {
	tl.truncateCalled = true
	return tl.truncateErr
}

func (tl *testLogger) Record(result *sqltypes.Result, selection *fivetransdk.SchemaSelection, selection2 *fivetransdk.TableSelection, operation lib.Operation) error {
	// TODO implement me
	panic("implement me")
}

func (tl *testLogger) State(state lib.SyncState) error {
	tl.states = append(tl.states, state)
	return nil
}

func (tl *testLogger) Release() {
	// TODO implement me
	panic("implement me")
}
