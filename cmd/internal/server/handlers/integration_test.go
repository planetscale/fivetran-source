//go:build integration

package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	fivetransdk "github.com/planetscale/fivetran-source/fivetran_sdk.v2"
	"github.com/planetscale/fivetran-source/lib"
	"google.golang.org/protobuf/proto"
)

func TestIntegrationBasicInsertUpdateDelete(t *testing.T) {
	psc := loadIntegrationSource(t)
	ctx := context.Background()
	tableName := integrationTableName(t)

	db := openIntegrationSQL(t, psc)
	t.Cleanup(func() {
		if _, err := db.ExecContext(context.Background(), "drop table if exists "+quoteIdent(tableName)); err != nil {
			t.Logf("drop integration table %s: %v", tableName, err)
		}
		_ = db.Close()
	})

	mustExec(t, ctx, db, fmt.Sprintf(`
		create table %s (
			id bigint primary key,
			name varchar(64) not null,
			qty int not null,
			flag tinyint(1) not null
		)`, quoteIdent(tableName)))
	mustExec(t, ctx, db, fmt.Sprintf(
		"insert into %s (id, name, qty, flag) values (1, 'one', 10, 1), (2, 'two', 20, 0)",
		quoteIdent(tableName),
	))

	model := map[int64]map[string]any{}
	first, state := runIntegrationSync(t, psc, tableName, []string{"id", "name", "qty", "flag"}, nil)
	applyIntegrationRecords(t, model, first.recordsForTable(tableName))
	assertIntegrationRows(t, model, map[int64]map[string]any{
		1: {"id": int64(1), "name": "one", "qty": int64(10), "flag": true},
		2: {"id": int64(2), "name": "two", "qty": int64(20), "flag": false},
	})

	mustExec(t, ctx, db, fmt.Sprintf("update %s set qty = 11 where id = 1", quoteIdent(tableName)))
	mustExec(t, ctx, db, fmt.Sprintf("delete from %s where id = 2", quoteIdent(tableName)))
	mustExec(t, ctx, db, fmt.Sprintf(
		"insert into %s (id, name, qty, flag) values (3, 'three', 30, 1)",
		quoteIdent(tableName),
	))

	second, _ := runIntegrationSync(t, psc, tableName, []string{"id", "name", "qty", "flag"}, state)
	applyIntegrationRecords(t, model, second.recordsForTable(tableName))
	assertIntegrationRows(t, model, map[int64]map[string]any{
		1: {"id": int64(1), "name": "one", "qty": int64(11), "flag": true},
		3: {"id": int64(3), "name": "three", "qty": int64(30), "flag": true},
	})
}

func TestIntegrationBitValues(t *testing.T) {
	psc := loadIntegrationSource(t)
	ctx := context.Background()
	tableName := integrationTableName(t)

	db := openIntegrationSQL(t, psc)
	t.Cleanup(func() {
		if _, err := db.ExecContext(context.Background(), "drop table if exists "+quoteIdent(tableName)); err != nil {
			t.Logf("drop integration table %s: %v", tableName, err)
		}
		_ = db.Close()
	})

	mustExec(t, ctx, db, fmt.Sprintf(`
		create table %s (
			id bigint primary key,
			b1 bit(1) not null,
			b8 bit(8) not null,
			b16 bit(16) not null
		)`, quoteIdent(tableName)))
	mustExec(t, ctx, db, fmt.Sprintf(
		"insert into %s (id, b1, b8, b16) values (1, b'1', b'10000000', b'0000000100000000')",
		quoteIdent(tableName),
	))

	model := map[int64]map[string]any{}
	first, _ := runIntegrationSync(t, psc, tableName, []string{"id", "b1", "b8", "b16"}, nil)
	for _, record := range first.recordsForTable(tableName) {
		t.Logf("bit record type=%s data=%s", record.Type, integrationDebugData(record.Data))
	}
	applyIntegrationRecords(t, model, first.recordsForTable(tableName))
	assertIntegrationRows(t, model, map[int64]map[string]any{
		1: {"id": int64(1), "b1": int64(1), "b8": int64(128), "b16": int64(256)},
	})
}

func TestIntegrationEnumSetSchemaValues(t *testing.T) {
	psc := loadIntegrationSource(t)
	ctx := context.Background()
	tableName := integrationTableName(t)

	db := openIntegrationSQL(t, psc)
	t.Cleanup(func() {
		if _, err := db.ExecContext(context.Background(), "drop table if exists "+quoteIdent(tableName)); err != nil {
			t.Logf("drop integration table %s: %v", tableName, err)
		}
		_ = db.Close()
	})

	mustExec(t, ctx, db, fmt.Sprintf(`
		create table %s (
			id bigint primary key,
			status enum('plain','with,comma','owner''s choice') not null,
			permissions set('read','write','owner''s choice') not null
		)`, quoteIdent(tableName)))

	mysqlClient, err := lib.NewMySQL(&psc)
	if err != nil {
		t.Fatalf("create mysql client: %v", err)
	}
	defer mysqlClient.Close()

	schemaBuilder := NewSchemaBuilder(psc.TreatTinyIntAsBoolean)
	if err := mysqlClient.BuildSchema(ctx, psc, schemaBuilder); err != nil {
		t.Fatalf("build schema: %v", err)
	}
	sourceSchema, err := schemaBuilder.(*FiveTranSchemaBuilder).BuildUpdateResponse()
	if err != nil {
		t.Fatalf("build update schema: %v", err)
	}

	enumValues := sourceSchema.EnumsAndSets[psc.Database][tableName]["status"].values
	setValues := sourceSchema.EnumsAndSets[psc.Database][tableName]["permissions"].values
	assertIntegrationStrings(t, enumValues, []string{"plain", "with,comma", "owner's choice"})
	assertIntegrationStrings(t, setValues, []string{"read", "write", "owner's choice"})
}

func TestIntegrationScalarTypes(t *testing.T) {
	psc := loadIntegrationSource(t)
	ctx := context.Background()
	tableName := integrationTableName(t)

	db := openIntegrationSQL(t, psc)
	t.Cleanup(func() {
		if _, err := db.ExecContext(context.Background(), "drop table if exists "+quoteIdent(tableName)); err != nil {
			t.Logf("drop integration table %s: %v", tableName, err)
		}
		_ = db.Close()
	})

	mustExec(t, ctx, db, fmt.Sprintf(`
		create table %s (
			id bigint primary key,
			dec_col decimal(20,4) not null,
			json_col json not null,
			bin_col varbinary(16) not null,
			date_col date not null,
			datetime_col datetime(6) not null,
			timestamp_col timestamp(6) null,
			nullable_col varchar(32) null
		)`, quoteIdent(tableName)))
	mustExec(t, ctx, db, fmt.Sprintf(
		`insert into %s (id, dec_col, json_col, bin_col, date_col, datetime_col, timestamp_col, nullable_col)
		 values (1, 1234567890.1234, json_object('a', 1, 'b', 'two'), x'00ff41', '2024-02-29', '2024-02-29 12:34:56.123456', '2024-02-29 12:34:56.123456', null)`,
		quoteIdent(tableName),
	))

	first, _ := runIntegrationSync(t, psc, tableName, []string{"id", "dec_col", "json_col", "bin_col", "date_col", "datetime_col", "timestamp_col", "nullable_col"}, nil)
	records := first.recordsForTable(tableName)
	for _, record := range records {
		t.Logf("scalar record type=%s data=%s", record.Type, integrationDebugData(record.Data))
	}

	model := map[int64]map[string]any{}
	applyIntegrationRecords(t, model, records)
	assertIntegrationRows(t, model, map[int64]map[string]any{
		1: {
			"id":            int64(1),
			"dec_col":       "1234567890.1234",
			"json_col":      `{"a": 1, "b": "two"}`,
			"bin_col":       []byte{0x00, 0xff, 0x41},
			"date_col":      "2024-02-29",
			"datetime_col":  "2024-02-29T12:34:56.123456Z",
			"timestamp_col": "2024-02-29T12:34:56.123456Z",
			"nullable_col":  nil,
		},
	})
}

func TestIntegrationIntermittentEmptySyncs(t *testing.T) {
	psc := loadIntegrationSource(t)
	ctx := context.Background()
	tableName := integrationTableName(t)

	db := openIntegrationSQL(t, psc)
	t.Cleanup(func() {
		if _, err := db.ExecContext(context.Background(), "drop table if exists "+quoteIdent(tableName)); err != nil {
			t.Logf("drop integration table %s: %v", tableName, err)
		}
		_ = db.Close()
	})

	mustExec(t, ctx, db, fmt.Sprintf(`
		create table %s (
			id bigint primary key,
			version int not null,
			payload varchar(128) not null,
			active tinyint(1) not null
		)`, quoteIdent(tableName)))

	columns := []string{"id", "version", "payload", "active"}
	expected := map[int64]map[string]any{}
	insertIntegrationLoadRows(t, ctx, db, tableName, 1, 1, 0, expected)

	model := map[int64]map[string]any{}
	first, state := runIntegrationSync(t, psc, tableName, columns, nil)
	applyIntegrationRecords(t, model, first.recordsForTable(tableName))
	assertIntegrationRows(t, model, expected)

	idle, state := runIntegrationSync(t, psc, tableName, columns, state)
	assertIntegrationRecordCount(t, idle.recordsForTable(tableName), 0)

	updateIntegrationLoadRow(t, ctx, db, tableName, 1, 1, true, expected)
	second, state := runIntegrationSync(t, psc, tableName, columns, state)
	applyIntegrationRecords(t, model, second.recordsForTable(tableName))
	assertIntegrationRows(t, model, expected)

	idle, _ = runIntegrationSync(t, psc, tableName, columns, state)
	assertIntegrationRecordCount(t, idle.recordsForTable(tableName), 0)
}

func TestIntegrationRepeatedSyncBursts(t *testing.T) {
	psc := loadIntegrationSource(t)
	ctx := context.Background()
	tableName := integrationTableName(t)

	db := openIntegrationSQL(t, psc)
	t.Cleanup(func() {
		if _, err := db.ExecContext(context.Background(), "drop table if exists "+quoteIdent(tableName)); err != nil {
			t.Logf("drop integration table %s: %v", tableName, err)
		}
		_ = db.Close()
	})

	mustExec(t, ctx, db, fmt.Sprintf(`
		create table %s (
			id bigint primary key,
			version int not null,
			payload varchar(128) not null,
			active tinyint(1) not null
		)`, quoteIdent(tableName)))

	columns := []string{"id", "version", "payload", "active"}
	expected := map[int64]map[string]any{}
	insertIntegrationLoadRows(t, ctx, db, tableName, 1, 24, 0, expected)

	model := map[int64]map[string]any{}
	sender, state := runIntegrationSync(t, psc, tableName, columns, nil)
	applyIntegrationRecords(t, model, sender.recordsForTable(tableName))
	assertIntegrationRows(t, model, expected)

	nextID := int64(25)
	for cycle := 1; cycle <= 4; cycle++ {
		for id := int64(1); id < nextID; id++ {
			if _, ok := expected[id]; !ok || id%4 != int64(cycle%4) {
				continue
			}
			updateIntegrationLoadRow(t, ctx, db, tableName, id, cycle, cycle%2 == 0, expected)
		}

		for id := int64(cycle); id < nextID; id += 9 {
			if _, ok := expected[id]; !ok {
				continue
			}
			deleteIntegrationLoadRow(t, ctx, db, tableName, id, expected)
		}

		insertIntegrationLoadRows(t, ctx, db, tableName, nextID, 8, cycle, expected)
		nextID += 8

		sender, state = runIntegrationSync(t, psc, tableName, columns, state)
		applyIntegrationRecords(t, model, sender.recordsForTable(tableName))
		assertIntegrationRows(t, model, expected)

		idle, nextState := runIntegrationSync(t, psc, tableName, columns, state)
		assertIntegrationRecordCount(t, idle.recordsForTable(tableName), 0)
		state = nextState
	}
}

func TestIntegrationStressBurstLoad(t *testing.T) {
	if !integrationStressEnabled() {
		t.Skip("set INTEGRATION_STRESS=1 to run stress integration tests")
	}

	psc := loadIntegrationSource(t)
	ctx := context.Background()
	tableName := integrationTableName(t)

	rows := integrationEnvInt(t, "INTEGRATION_STRESS_ROWS", 500)
	cycles := integrationEnvInt(t, "INTEGRATION_STRESS_CYCLES", 3)
	if rows < 10 {
		t.Fatalf("INTEGRATION_STRESS_ROWS must be at least 10, got %d", rows)
	}
	if cycles < 1 {
		t.Fatalf("INTEGRATION_STRESS_CYCLES must be at least 1, got %d", cycles)
	}

	db := openIntegrationSQL(t, psc)
	t.Cleanup(func() {
		if _, err := db.ExecContext(context.Background(), "drop table if exists "+quoteIdent(tableName)); err != nil {
			t.Logf("drop integration table %s: %v", tableName, err)
		}
		_ = db.Close()
	})

	mustExec(t, ctx, db, fmt.Sprintf(`
		create table %s (
			id bigint primary key,
			version int not null,
			payload varchar(128) not null,
			active tinyint(1) not null
		)`, quoteIdent(tableName)))

	columns := []string{"id", "version", "payload", "active"}
	expected := map[int64]map[string]any{}
	insertIntegrationLoadRows(t, ctx, db, tableName, 1, int64(rows), 0, expected)

	model := map[int64]map[string]any{}
	sender, state := runIntegrationSync(t, psc, tableName, columns, nil)
	applyIntegrationRecords(t, model, sender.recordsForTable(tableName))
	assertIntegrationRowCount(t, model, len(expected))
	assertIntegrationRowsContain(t, model, expected, integrationSampleIDs(expected, 10))

	nextID := int64(rows) + 1
	for cycle := 1; cycle <= cycles; cycle++ {
		updateLimit := rows / 3
		if updateLimit > 250 {
			updateLimit = 250
		}
		updated := 0
		for id := int64(1); id < nextID && updated < updateLimit; id++ {
			if _, ok := expected[id]; !ok || id%int64(cycle+2) != 0 {
				continue
			}
			updateIntegrationLoadRow(t, ctx, db, tableName, id, cycle, (id+int64(cycle))%2 == 0, expected)
			updated++
		}

		deleteLimit := rows / 10
		if deleteLimit > 100 {
			deleteLimit = 100
		}
		deleted := 0
		for id := int64(cycle); id < nextID && deleted < deleteLimit; id += int64(cycle + 7) {
			if _, ok := expected[id]; !ok {
				continue
			}
			deleteIntegrationLoadRow(t, ctx, db, tableName, id, expected)
			deleted++
		}

		insertCount := rows / 5
		if insertCount > 250 {
			insertCount = 250
		}
		insertIntegrationLoadRows(t, ctx, db, tableName, nextID, int64(insertCount), cycle, expected)
		nextID += int64(insertCount)

		sender, state = runIntegrationSync(t, psc, tableName, columns, state)
		applyIntegrationRecords(t, model, sender.recordsForTable(tableName))
		assertIntegrationRowCount(t, model, len(expected))
		assertIntegrationRowsContain(t, model, expected, integrationSampleIDs(expected, 10))

		idle, nextState := runIntegrationSync(t, psc, tableName, columns, state)
		assertIntegrationRecordCount(t, idle.recordsForTable(tableName), 0)
		state = nextState
	}
}

func loadIntegrationSource(t *testing.T) lib.PlanetScaleSource {
	t.Helper()

	var psc lib.PlanetScaleSource
	if path := os.Getenv("PS_INTEGRATION_CONFIG"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read PS_INTEGRATION_CONFIG: %v", err)
		}
		if err := json.Unmarshal(data, &psc); err != nil {
			t.Fatalf("parse PS_INTEGRATION_CONFIG: %v", err)
		}
	} else {
		psc = lib.PlanetScaleSource{
			Host:     os.Getenv("DATABASE_HOST"),
			Database: os.Getenv("DATABASE_NAME"),
			Username: os.Getenv("DATABASE_USERNAME"),
			Password: os.Getenv("DATABASE_PASSWORD"),
		}
	}

	missing := []string{}
	if psc.Host == "" {
		missing = append(missing, "DATABASE_HOST")
	}
	if psc.Database == "" {
		missing = append(missing, "DATABASE_NAME")
	}
	if psc.Username == "" {
		missing = append(missing, "DATABASE_USERNAME")
	}
	if psc.Password == "" {
		missing = append(missing, "DATABASE_PASSWORD")
	}
	if len(missing) > 0 {
		t.Skipf("integration credentials are not configured; set PS_INTEGRATION_CONFIG or %s", strings.Join(missing, ", "))
	}

	psc.TreatTinyIntAsBoolean = true
	if raw := os.Getenv("DATABASE_TREAT_TINY_INT_AS_BOOLEAN"); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			t.Fatalf("parse DATABASE_TREAT_TINY_INT_AS_BOOLEAN: %v", err)
		}
		psc.TreatTinyIntAsBoolean = v
	}
	if raw := os.Getenv("DATABASE_USE_REPLICA"); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			t.Fatalf("parse DATABASE_USE_REPLICA: %v", err)
		}
		psc.UseReplica = v
	}

	return psc
}

func openIntegrationSQL(t *testing.T, psc lib.PlanetScaleSource) *sql.DB {
	t.Helper()

	db, err := sql.Open("mysql", psc.DSN(psdbconnect.TabletType_primary))
	if err != nil {
		t.Fatalf("open mysql connection: %v", err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		t.Fatalf("ping mysql connection: %v", err)
	}
	return db
}

func runIntegrationSync(t *testing.T, psc lib.PlanetScaleSource, tableName string, columns []string, state *lib.SyncState) (*integrationSender, *lib.SyncState) {
	t.Helper()

	mysqlClient, err := lib.NewMySQL(&psc)
	if err != nil {
		t.Fatalf("create mysql client: %v", err)
	}
	defer mysqlClient.Close()

	connectClient := lib.NewConnectClient(&mysqlClient)
	schemaBuilder := NewSchemaBuilder(psc.TreatTinyIntAsBoolean)
	if err := mysqlClient.BuildSchema(context.Background(), psc, schemaBuilder); err != nil {
		t.Fatalf("build schema: %v", err)
	}
	sourceSchema, err := schemaBuilder.(*FiveTranSchemaBuilder).BuildUpdateResponse()
	if err != nil {
		t.Fatalf("build update schema: %v", err)
	}

	if state == nil {
		shards, err := connectClient.ListShards(context.Background(), psc)
		if err != nil {
			t.Fatalf("list shards: %v", err)
		}
		shardState, err := psc.GetInitialState(psc.Database, shards)
		if err != nil {
			t.Fatalf("build initial state: %v", err)
		}
		state = &lib.SyncState{
			Keyspaces: map[string]lib.KeyspaceState{
				psc.Database: {
					Streams: map[string]lib.ShardStates{
						psc.Database + ":" + tableName: shardState,
					},
				},
			},
		}
	}

	selection := integrationSelection(psc.Database, tableName, columns)
	sender := &integrationSender{}
	logger := NewSchemaAwareSerializer(sender, "integration", psc.TreatTinyIntAsBoolean, sourceSchema.SchemaList, sourceSchema.EnumsAndSets)
	syncer := &Sync{}
	if err := syncer.Handle(&psc, &connectClient, logger, state, selection); err != nil {
		t.Fatalf("run sync: %v", err)
	}
	if checkpoint, ok := sender.latestState(t); ok {
		state = checkpoint
	}
	return sender, state
}

func integrationSelection(schemaName, tableName string, columns []string) *fivetransdk.Selection_WithSchema {
	selectedColumns := map[string]bool{}
	for _, column := range columns {
		selectedColumns[column] = true
	}
	return &fivetransdk.Selection_WithSchema{
		WithSchema: &fivetransdk.TablesWithSchema{
			Schemas: []*fivetransdk.SchemaSelection{
				{
					SchemaName: schemaName,
					Included:   true,
					Tables: []*fivetransdk.TableSelection{
						{
							TableName: tableName,
							Included:  true,
							Columns:   selectedColumns,
						},
					},
				},
			},
		},
	}
}

type integrationSender struct {
	responses []*fivetransdk.UpdateResponse
}

func (s *integrationSender) Send(response *fivetransdk.UpdateResponse) error {
	s.responses = append(s.responses, proto.Clone(response).(*fivetransdk.UpdateResponse))
	return nil
}

func (s *integrationSender) latestState(t *testing.T) (*lib.SyncState, bool) {
	t.Helper()

	for i := len(s.responses) - 1; i >= 0; i-- {
		checkpoint := s.responses[i].GetCheckpoint()
		if checkpoint == nil {
			continue
		}
		var state lib.SyncState
		if err := json.Unmarshal([]byte(checkpoint.StateJson), &state); err != nil {
			t.Fatalf("parse checkpoint state: %v", err)
		}
		return &state, true
	}
	return nil, false
}

func (s *integrationSender) recordsForTable(tableName string) []*fivetransdk.Record {
	records := []*fivetransdk.Record{}
	for _, response := range s.responses {
		record := response.GetRecord()
		if record == nil || record.TableName != tableName {
			continue
		}
		records = append(records, record)
	}
	return records
}

func applyIntegrationRecords(t *testing.T, rows map[int64]map[string]any, records []*fivetransdk.Record) {
	t.Helper()

	for _, record := range records {
		if record.Type == fivetransdk.RecordType_TRUNCATE {
			for id := range rows {
				delete(rows, id)
			}
			continue
		}

		idValue, ok := record.Data["id"]
		if !ok {
			t.Fatalf("record missing id: %+v", record)
		}
		id, ok := integrationValue(idValue).(int64)
		if !ok {
			t.Fatalf("record id is not int64: %T %v", integrationValue(idValue), integrationValue(idValue))
		}
		if record.Type == fivetransdk.RecordType_DELETE {
			delete(rows, id)
			continue
		}

		if _, ok := rows[id]; !ok {
			rows[id] = map[string]any{}
		}
		for column, value := range record.Data {
			rows[id][column] = integrationValue(value)
		}
	}
}

func integrationValue(value *fivetransdk.ValueType) any {
	switch inner := value.Inner.(type) {
	case *fivetransdk.ValueType_Bool:
		return inner.Bool
	case *fivetransdk.ValueType_Int:
		return int64(inner.Int)
	case *fivetransdk.ValueType_Long:
		return inner.Long
	case *fivetransdk.ValueType_String_:
		return inner.String_
	case *fivetransdk.ValueType_Decimal:
		return inner.Decimal
	case *fivetransdk.ValueType_Json:
		return inner.Json
	case *fivetransdk.ValueType_Binary:
		return inner.Binary
	case *fivetransdk.ValueType_NaiveDate:
		return inner.NaiveDate.AsTime().Format(time.DateOnly)
	case *fivetransdk.ValueType_NaiveDatetime:
		return inner.NaiveDatetime.AsTime().Format(time.RFC3339Nano)
	case *fivetransdk.ValueType_UtcDatetime:
		return inner.UtcDatetime.AsTime().Format(time.RFC3339Nano)
	case *fivetransdk.ValueType_Null:
		return nil
	default:
		return fmt.Sprintf("%v", value)
	}
}

func integrationDebugData(data map[string]*fivetransdk.ValueType) string {
	parts := make([]string, 0, len(data))
	for column, value := range data {
		parts = append(parts, column+"="+integrationDebugValue(value))
	}
	return strings.Join(parts, ", ")
}

func integrationDebugValue(value *fivetransdk.ValueType) string {
	switch inner := value.Inner.(type) {
	case *fivetransdk.ValueType_Bool:
		return fmt.Sprintf("bool(%v)", inner.Bool)
	case *fivetransdk.ValueType_Int:
		return fmt.Sprintf("int(%d)", inner.Int)
	case *fivetransdk.ValueType_Long:
		return fmt.Sprintf("long(%d)", inner.Long)
	case *fivetransdk.ValueType_String_:
		return fmt.Sprintf("string(%q)", inner.String_)
	case *fivetransdk.ValueType_Json:
		return fmt.Sprintf("json(%q)", inner.Json)
	case *fivetransdk.ValueType_Binary:
		return fmt.Sprintf("binary(%x)", inner.Binary)
	case *fivetransdk.ValueType_Decimal:
		return fmt.Sprintf("decimal(%q)", inner.Decimal)
	case *fivetransdk.ValueType_NaiveDate:
		return fmt.Sprintf("date(%s)", inner.NaiveDate.AsTime().Format(time.DateOnly))
	case *fivetransdk.ValueType_NaiveDatetime:
		return fmt.Sprintf("datetime(%s)", inner.NaiveDatetime.AsTime().Format(time.RFC3339Nano))
	case *fivetransdk.ValueType_UtcDatetime:
		return fmt.Sprintf("timestamp(%s)", inner.UtcDatetime.AsTime().Format(time.RFC3339Nano))
	case *fivetransdk.ValueType_Null:
		return "null"
	default:
		return fmt.Sprintf("%T(%v)", value.Inner, value)
	}
}

func assertIntegrationRows(t *testing.T, got, want map[int64]map[string]any) {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		gotJSON, _ := json.MarshalIndent(got, "", "  ")
		wantJSON, _ := json.MarshalIndent(want, "", "  ")
		t.Fatalf("unexpected materialized rows\nwant: %s\ngot:  %s", wantJSON, gotJSON)
	}
}

func assertIntegrationStrings(t *testing.T, got, want []string) {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected strings\nwant: %#v\ngot:  %#v", want, got)
	}
}

func assertIntegrationRecordCount(t *testing.T, records []*fivetransdk.Record, want int) {
	t.Helper()

	if len(records) != want {
		t.Fatalf("unexpected record count: want %d, got %d: %s", want, len(records), strings.Join(integrationRecordSummaries(records), "; "))
	}
}

func assertIntegrationRowCount(t *testing.T, rows map[int64]map[string]any, want int) {
	t.Helper()

	if len(rows) != want {
		t.Fatalf("unexpected row count: want %d, got %d", want, len(rows))
	}
}

func assertIntegrationRowsContain(t *testing.T, got, want map[int64]map[string]any, ids []int64) {
	t.Helper()

	for _, id := range ids {
		if !reflect.DeepEqual(got[id], want[id]) {
			gotJSON, _ := json.Marshal(got[id])
			wantJSON, _ := json.Marshal(want[id])
			t.Fatalf("unexpected row %d\nwant: %s\ngot:  %s", id, wantJSON, gotJSON)
		}
	}
}

func insertIntegrationLoadRows(t *testing.T, ctx context.Context, db *sql.DB, tableName string, startID, count int64, version int, rows map[int64]map[string]any) {
	t.Helper()

	const batchSize = int64(100)
	for offset := int64(0); offset < count; {
		n := batchSize
		if remaining := count - offset; remaining < n {
			n = remaining
		}

		placeholders := make([]string, 0, int(n))
		args := make([]any, 0, int(n)*4)
		for i := int64(0); i < n; i++ {
			id := startID + offset + i
			payload := integrationPayload(version, id)
			active := id%2 == 0

			placeholders = append(placeholders, "(?, ?, ?, ?)")
			args = append(args, id, version, payload, integrationBoolInt(active))
			rows[id] = map[string]any{
				"id":      id,
				"version": int64(version),
				"payload": payload,
				"active":  active,
			}
		}

		mustExec(t, ctx, db, fmt.Sprintf(
			"insert into %s (id, version, payload, active) values %s",
			quoteIdent(tableName),
			strings.Join(placeholders, ", "),
		), args...)
		offset += n
	}
}

func updateIntegrationLoadRow(t *testing.T, ctx context.Context, db *sql.DB, tableName string, id int64, version int, active bool, rows map[int64]map[string]any) {
	t.Helper()

	payload := integrationPayload(version, id)
	mustExec(t, ctx, db, fmt.Sprintf(
		"update %s set version = ?, payload = ?, active = ? where id = ?",
		quoteIdent(tableName),
	), version, payload, integrationBoolInt(active), id)

	rows[id] = map[string]any{
		"id":      id,
		"version": int64(version),
		"payload": payload,
		"active":  active,
	}
}

func deleteIntegrationLoadRow(t *testing.T, ctx context.Context, db *sql.DB, tableName string, id int64, rows map[int64]map[string]any) {
	t.Helper()

	mustExec(t, ctx, db, fmt.Sprintf("delete from %s where id = ?", quoteIdent(tableName)), id)
	delete(rows, id)
}

func integrationRecordSummaries(records []*fivetransdk.Record) []string {
	limit := len(records)
	if limit > 10 {
		limit = 10
	}

	summaries := make([]string, 0, limit+1)
	for i := 0; i < limit; i++ {
		summaries = append(summaries, fmt.Sprintf("%s %s", records[i].Type, integrationDebugData(records[i].Data)))
	}
	if len(records) > limit {
		summaries = append(summaries, fmt.Sprintf("... %d more", len(records)-limit))
	}
	return summaries
}

func integrationSampleIDs(rows map[int64]map[string]any, max int) []int64 {
	ids := make([]int64, 0, max)
	for id := range rows {
		ids = append(ids, id)
		if len(ids) == max {
			break
		}
	}
	return ids
}

func integrationPayload(version int, id int64) string {
	return fmt.Sprintf("payload-%03d-%06d", version, id)
}

func integrationBoolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func integrationStressEnabled() bool {
	value, err := strconv.ParseBool(os.Getenv("INTEGRATION_STRESS"))
	return err == nil && value
}

func integrationEnvInt(t *testing.T, name string, defaultValue int) int {
	t.Helper()

	raw := os.Getenv(name)
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return value
}

func mustExec(t *testing.T, ctx context.Context, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("exec query %q: %v", query, err)
	}
}

func integrationTableName(t *testing.T) string {
	t.Helper()
	name := strings.ToLower(strings.TrimPrefix(t.Name(), "Test"))
	replacer := strings.NewReplacer("/", "_", "-", "_")
	name = replacer.Replace(name)
	if len(name) > 24 {
		name = name[:24]
	}
	return "fs_integration_" + name + "_" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

func quoteIdent(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}
