package handlers

import (
	fivetransdk "github.com/planetscale/fivetran-proto/go"
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

type testLogger struct{}

func (testLogger) Info(s string) {
	// TODO implement me
	panic("implement me")
}

func (testLogger) Log(level fivetransdk.LogLevel, s string) error {
	// TODO implement me
	panic("implement me")
}

func (testLogger) Record(result *sqltypes.Result, selection *fivetransdk.SchemaSelection, selection2 *fivetransdk.TableSelection) error {
	// TODO implement me
	panic("implement me")
}

func (testLogger) State(state lib.SyncState) error {
	return nil
}

func (testLogger) Release() {
	// TODO implement me
	panic("implement me")
}
