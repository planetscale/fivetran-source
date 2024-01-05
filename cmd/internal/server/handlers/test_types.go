package handlers

import (
	"fmt"

	"github.com/planetscale/connect-sdk/lib"
	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk"
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
}

func (testLogger) Info(s string) {
	// TODO implement me
	panic("implement me")
}

func (tl *testLogger) Log(level fivetransdk.LogLevel, s string) error {
	// TODO implement me
	panic("implement me")
}

func (tl *testLogger) Update(*lib.UpdatedRow, *fivetransdk.SchemaSelection, *fivetransdk.TableSelection) error {
	return fmt.Errorf("%v is not implemented", "Update")
}

func (tl *testLogger) Truncate(*fivetransdk.SchemaSelection, *fivetransdk.TableSelection) error {
	tl.truncateCalled = true
	return nil
}

func (tl *testLogger) Record(result *sqltypes.Result, selection *fivetransdk.SchemaSelection, selection2 *fivetransdk.TableSelection, operation lib.Operation) error {
	// TODO implement me
	panic("implement me")
}

func (tl *testLogger) State(state lib.SyncState) error {
	return nil
}

func (tl *testLogger) Release() {
	// TODO implement me
	panic("implement me")
}
