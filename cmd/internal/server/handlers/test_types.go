package handlers

import (
	"fmt"

	fivetransdk_v2 "github.com/planetscale/fivetran-sdk-grpc/go"

	"github.com/planetscale/fivetran-source/lib"
	"vitess.io/vitess/go/sqltypes"
)

type testLogSender struct {
	sendError    error
	lastResponse *fivetransdk_v2.UpdateResponse
}

func (l *testLogSender) Send(response *fivetransdk_v2.UpdateResponse) error {
	l.lastResponse = response
	return l.sendError
}

type testLogger struct{}

func (testLogger) Info(s string) {
	// TODO implement me
	panic("implement me")
}

func (testLogger) Log(level fivetransdk_v2.LogLevel, s string) error {
	// TODO implement me
	panic("implement me")
}

func (testLogger) Update(*lib.UpdatedRow, *fivetransdk_v2.SchemaSelection, *fivetransdk_v2.TableSelection) error {
	return fmt.Errorf("%v is not implemented", "Update")
}

func (testLogger) Record(result *sqltypes.Result, selection *fivetransdk_v2.SchemaSelection, selection2 *fivetransdk_v2.TableSelection, operation lib.Operation) error {
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
