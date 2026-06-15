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

func TestBugbashBasicInsertUpdateDelete(t *testing.T) {
	psc := loadBugbashSource(t)
	ctx := context.Background()
	tableName := bugbashTableName(t)

	db := openBugbashSQL(t, psc)
	t.Cleanup(func() {
		if _, err := db.ExecContext(context.Background(), "drop table if exists "+quoteIdent(tableName)); err != nil {
			t.Logf("drop bugbash table %s: %v", tableName, err)
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
	first, state := runBugbashSync(t, psc, tableName, []string{"id", "name", "qty", "flag"}, nil)
	applyBugbashRecords(t, model, first.recordsForTable(tableName))
	assertBugbashRows(t, model, map[int64]map[string]any{
		1: {"id": int64(1), "name": "one", "qty": int64(10), "flag": true},
		2: {"id": int64(2), "name": "two", "qty": int64(20), "flag": false},
	})

	mustExec(t, ctx, db, fmt.Sprintf("update %s set qty = 11 where id = 1", quoteIdent(tableName)))
	mustExec(t, ctx, db, fmt.Sprintf("delete from %s where id = 2", quoteIdent(tableName)))
	mustExec(t, ctx, db, fmt.Sprintf(
		"insert into %s (id, name, qty, flag) values (3, 'three', 30, 1)",
		quoteIdent(tableName),
	))

	second, _ := runBugbashSync(t, psc, tableName, []string{"id", "name", "qty", "flag"}, state)
	applyBugbashRecords(t, model, second.recordsForTable(tableName))
	assertBugbashRows(t, model, map[int64]map[string]any{
		1: {"id": int64(1), "name": "one", "qty": int64(11), "flag": true},
		3: {"id": int64(3), "name": "three", "qty": int64(30), "flag": true},
	})
}

func loadBugbashSource(t *testing.T) lib.PlanetScaleSource {
	t.Helper()

	var psc lib.PlanetScaleSource
	if path := os.Getenv("PS_BUGBASH_CONFIG"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read PS_BUGBASH_CONFIG: %v", err)
		}
		if err := json.Unmarshal(data, &psc); err != nil {
			t.Fatalf("parse PS_BUGBASH_CONFIG: %v", err)
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
		t.Skipf("bugbash credentials are not configured; set PS_BUGBASH_CONFIG or %s", strings.Join(missing, ", "))
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

func openBugbashSQL(t *testing.T, psc lib.PlanetScaleSource) *sql.DB {
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

func runBugbashSync(t *testing.T, psc lib.PlanetScaleSource, tableName string, columns []string, state *lib.SyncState) (*bugbashSender, *lib.SyncState) {
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

	selection := bugbashSelection(psc.Database, tableName, columns)
	sender := &bugbashSender{}
	logger := NewSchemaAwareSerializer(sender, "bugbash", psc.TreatTinyIntAsBoolean, sourceSchema.SchemaList, sourceSchema.EnumsAndSets)
	syncer := &Sync{}
	if err := syncer.Handle(&psc, &connectClient, logger, state, selection); err != nil {
		t.Fatalf("run sync: %v", err)
	}
	if checkpoint, ok := sender.latestState(t); ok {
		state = checkpoint
	}
	return sender, state
}

func bugbashSelection(schemaName, tableName string, columns []string) *fivetransdk.Selection_WithSchema {
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

type bugbashSender struct {
	responses []*fivetransdk.UpdateResponse
}

func (s *bugbashSender) Send(response *fivetransdk.UpdateResponse) error {
	s.responses = append(s.responses, proto.Clone(response).(*fivetransdk.UpdateResponse))
	return nil
}

func (s *bugbashSender) latestState(t *testing.T) (*lib.SyncState, bool) {
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

func (s *bugbashSender) recordsForTable(tableName string) []*fivetransdk.Record {
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

func applyBugbashRecords(t *testing.T, rows map[int64]map[string]any, records []*fivetransdk.Record) {
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
		id, ok := bugbashValue(idValue).(int64)
		if !ok {
			t.Fatalf("record id is not int64: %T %v", bugbashValue(idValue), bugbashValue(idValue))
		}
		if record.Type == fivetransdk.RecordType_DELETE {
			delete(rows, id)
			continue
		}

		if _, ok := rows[id]; !ok {
			rows[id] = map[string]any{}
		}
		for column, value := range record.Data {
			rows[id][column] = bugbashValue(value)
		}
	}
}

func bugbashValue(value *fivetransdk.ValueType) any {
	switch inner := value.Inner.(type) {
	case *fivetransdk.ValueType_Bool:
		return inner.Bool
	case *fivetransdk.ValueType_Int:
		return int64(inner.Int)
	case *fivetransdk.ValueType_Long:
		return inner.Long
	case *fivetransdk.ValueType_String_:
		return inner.String_
	case *fivetransdk.ValueType_Null:
		return nil
	default:
		return fmt.Sprintf("%v", value)
	}
}

func assertBugbashRows(t *testing.T, got, want map[int64]map[string]any) {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		gotJSON, _ := json.MarshalIndent(got, "", "  ")
		wantJSON, _ := json.MarshalIndent(want, "", "  ")
		t.Fatalf("unexpected materialized rows\nwant: %s\ngot:  %s", wantJSON, gotJSON)
	}
}

func mustExec(t *testing.T, ctx context.Context, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("exec query %q: %v", query, err)
	}
}

func bugbashTableName(t *testing.T) string {
	t.Helper()
	name := strings.ToLower(strings.TrimPrefix(t.Name(), "Test"))
	replacer := strings.NewReplacer("/", "_", "-", "_")
	name = replacer.Replace(name)
	if len(name) > 24 {
		name = name[:24]
	}
	return "fs_bugbash_" + name + "_" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

func quoteIdent(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}
