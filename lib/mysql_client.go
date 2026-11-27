package lib

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
)

type MysqlClient interface {
	BuildSchema(ctx context.Context, psc PlanetScaleSource, schemaBuilder SchemaBuilder) error
	PingContext(context.Context, PlanetScaleSource) error
	GetVitessShards(ctx context.Context, psc PlanetScaleSource) ([]string, error)
	GetKeyspaceTableColumns(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error)
	GetShardTableColumnOrder(ctx context.Context, keyspaceName, shard, tableName string, tabletType psdbconnect.TabletType) ([]string, error)
	Close() error
}

func NewMySQL(psc *PlanetScaleSource) (MysqlClient, error) {
	db, err := sql.Open("mysql", psc.DSN(psdbconnect.TabletType_primary))
	if err != nil {
		return nil, err
	}

	return mysqlClient{
		db:     db,
		psc:    *psc,
		shards: &shardDBs{dbs: map[string]*sql.DB{}},
	}, nil
}

type mysqlClient struct {
	db  *sql.DB
	psc PlanetScaleSource
	// shards holds one connection pool per shard target, opened on first
	// use (see GetShardTableColumnOrder) and closed by Close.
	shards *shardDBs
}

type shardDBs struct {
	mu  sync.Mutex
	dbs map[string]*sql.DB
}

// BuildSchema returns schemas for all tables in a PlanetScale database
// 1. Get all keyspaces for the PlanetScale database
// 2. Get the schemas for all tables in a keyspace, for each keyspace
// 2. Get columns and primary keys for each table from information_schema.columns
// 3. Format results into Fivetran response
func (p mysqlClient) BuildSchema(ctx context.Context, psc PlanetScaleSource, schemaBuilder SchemaBuilder) error {
	keyspaces, err := p.GetKeyspaces(ctx, psc)
	if err != nil {
		return errors.Wrap(err, "Unable to build schema for database")
	}

	for _, keyspaceName := range keyspaces {
		schemaBuilder.OnKeyspace(keyspaceName)
		tableNames, err := p.getKeyspaceTableNames(ctx, keyspaceName)
		if err != nil {
			return errors.Wrap(err, "Unable to build schema for database")
		}

		for _, tableName := range tableNames {
			schemaBuilder.OnTable(keyspaceName, tableName)

			columns, err := p.GetKeyspaceTableColumns(ctx, keyspaceName, tableName)
			if err != nil {
				return errors.Wrap(err, "Unable to build schema for database")
			}

			schemaBuilder.OnColumns(keyspaceName, tableName, columns)
		}
	}

	return nil
}

func (p mysqlClient) Close() error {
	if p.shards != nil {
		p.shards.mu.Lock()
		for target, db := range p.shards.dbs {
			_ = db.Close()
			delete(p.shards.dbs, target)
		}
		p.shards.mu.Unlock()
	}
	return p.db.Close()
}

// GetKeyspaceTableColumns returns the columns of a table in table order
// (ordinal_position): the sync compares that order between syncs (see
// ColumnOrderChange).
func (p mysqlClient) GetKeyspaceTableColumns(ctx context.Context, keyspaceName string, tableName string) ([]MysqlColumn, error) {
	var columns []MysqlColumn
	columnNamesQR, err := p.db.QueryContext(
		ctx,
		"select column_name, column_type, column_key from information_schema.columns where table_name=? AND table_schema=? order by ordinal_position;",
		tableName, keyspaceName,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to get column names & types for table %v", tableName)
	}
	for columnNamesQR.Next() {
		var (
			name       string
			columnType string
			columnKey  string
		)
		if err = columnNamesQR.Scan(&name, &columnType, &columnKey); err != nil {
			return nil, errors.Wrapf(err, "Unable to scan row for column names & types of table %v", tableName)
		}

		columns = append(columns, MysqlColumn{
			Name:         name,
			Type:         columnType,
			IsPrimaryKey: strings.EqualFold(columnKey, "PRI"),
		})
	}

	if err := columnNamesQR.Err(); err != nil {
		return nil, errors.Wrapf(err, "unable to iterate columns for table %s", tableName)
	}

	return columns, nil
}

// shardColumnOrderTimeout bounds the shard-targeted column query; a shard
// that does not answer in time gets the unknown-order path (schema history
// check), not a stuck sync.
const shardColumnOrderTimeout = 30 * time.Second

// shardConnMaxIdleTime closes a shard connection that the sync no longer
// uses (it moved on to other tables) instead of keeping it open for the rest
// of the sync.
const shardConnMaxIdleTime = time.Minute

// GetShardTableColumnOrder returns every column of a table, INVISIBLE ones
// included, in table order as one shard has them. Online DDL cuts over every
// shard on its own, so during a schema change the shards of a table can have
// different column orders, and vtgate answers information_schema queries
// from one of them.
//
// The query goes to the shard with Vitess shard targeting in the connection's
// database name, "<keyspace>:<shard>" (with the "@primary"/"@replica" suffix
// that DSN adds), the same way the integration tests read one shard
// (assertIntegrationRowsOnEveryShard). It is SHOW COLUMNS without a schema
// name, so the tablet answers for its own database whatever that is named;
// unlike the column names of a SELECT *, SHOW COLUMNS lists INVISIBLE columns,
// which binlog row events contain too.
func (p mysqlClient) GetShardTableColumnOrder(ctx context.Context, keyspaceName, shard, tableName string, tabletType psdbconnect.TabletType) ([]string, error) {
	db, err := p.shardDB(keyspaceName, shard, tabletType)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, shardColumnOrderTimeout)
	defer cancel()
	target := keyspaceName + ":" + shard
	rows, err := db.QueryContext(ctx, "show columns from "+quoteIdentifier(tableName))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to read the columns of table %s on shard %s", tableName, target)
	}
	defer rows.Close()
	fields, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to read the columns of table %s on shard %s", tableName, target)
	}
	if len(fields) == 0 {
		return nil, errors.Errorf("unable to read the columns of table %s on shard %s: SHOW COLUMNS returned no fields", tableName, target)
	}
	// The first field is the column name (Field); the rest are not needed.
	values := make([]sql.RawBytes, len(fields))
	dest := make([]any, len(fields))
	for i := range values {
		dest[i] = &values[i]
	}
	var order []string
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, errors.Wrapf(err, "unable to read the columns of table %s on shard %s", tableName, target)
		}
		order = append(order, string(values[0]))
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrapf(err, "unable to read the columns of table %s on shard %s", tableName, target)
	}
	return order, nil
}

// shardDB returns the connection pool targeted at keyspace:shard, opening it
// on first use.
func (p mysqlClient) shardDB(keyspaceName, shard string, tabletType psdbconnect.TabletType) (*sql.DB, error) {
	if p.shards == nil {
		return nil, errors.New("shard-targeted connections are not available")
	}
	key := keyspaceName + ":" + shard + "@" + tabletTypeToString(tabletType)
	p.shards.mu.Lock()
	defer p.shards.mu.Unlock()
	if db, ok := p.shards.dbs[key]; ok {
		return db, nil
	}
	psc := p.psc
	psc.Database = keyspaceName + ":" + shard
	db, err := sql.Open("mysql", psc.DSN(tabletType))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open a connection to shard %s:%s", keyspaceName, shard)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(shardConnMaxIdleTime)
	p.shards.dbs[key] = db
	return db, nil
}

func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func (p mysqlClient) GetVitessShards(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
	var shardNames []string

	shardNamesQR, err := p.db.QueryContext(
		ctx,
		`show vitess_shards like "`+psc.Database+`/%";`,
	)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to query database for shards")
	}

	for shardNamesQR.Next() {
		var name string
		if err = shardNamesQR.Scan(&name); err != nil {
			return nil, errors.Wrap(err, "unable to get shard names")
		}

		shardNames = append(shardNames, name)
	}

	if err := shardNamesQR.Err(); err != nil {
		return nil, errors.Wrapf(err, "unable to iterate shard names for %s", psc.Database)
	}
	return shardsForKeyspace(psc.Database, shardNames), nil
}

// shardsForKeyspace extracts the shard names belonging to exactly keyspace from
// the "<keyspace>/<shard>" rows returned by "show vitess_shards".
func shardsForKeyspace(keyspace string, shardNames []string) []string {
	var shards []string
	for _, name := range shardNames {
		ks, shard, ok := strings.Cut(name, "/")
		if !ok || ks != keyspace {
			continue
		}
		shards = append(shards, shard)
	}
	return shards
}

func (p mysqlClient) PingContext(ctx context.Context, psc PlanetScaleSource) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return p.db.PingContext(ctx)
}

func (p mysqlClient) getKeyspaceTableNames(ctx context.Context, keyspaceName string) ([]string, error) {
	var tables []string

	tableNamesQR, err := p.db.Query(fmt.Sprintf("show tables from `%s`;", keyspaceName))
	if err != nil {
		return tables, errors.Wrap(err, "Unable to query database for schema")
	}

	for tableNamesQR.Next() {
		var name string
		if err = tableNamesQR.Scan(&name); err != nil {
			return tables, errors.Wrap(err, "unable to get table names")
		}

		tables = append(tables, name)
	}

	if err := tableNamesQR.Err(); err != nil {
		return tables, errors.Wrap(err, "unable to iterate table rows")
	}

	return tables, err
}

func (p mysqlClient) GetKeyspaces(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
	var keyspaces []string

	// TODO: is there a prepared statement equivalent?
	shardNamesQR, err := p.db.QueryContext(
		ctx,
		`show vitess_keyspaces like "%`+psc.Database+`%";`,
	)
	if err != nil {
		return keyspaces, errors.Wrap(err, "Unable to query database for keyspaces")
	}

	for shardNamesQR.Next() {
		var name string
		if err = shardNamesQR.Scan(&name); err != nil {
			return keyspaces, errors.Wrap(err, "unable to get shard names")
		}

		keyspaces = append(keyspaces, strings.TrimPrefix(name, psc.Database+"/"))
	}

	if err := shardNamesQR.Err(); err != nil {
		return keyspaces, errors.Wrapf(err, "unable to iterate shard names for %s", psc.Database)
	}
	return keyspaces, nil
}
