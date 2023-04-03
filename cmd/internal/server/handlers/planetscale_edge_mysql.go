package handlers

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
)

type PlanetScaleEdgeMysqlAccess interface {
	PingContext(context.Context, PlanetScaleSource) error
	GetKeyspaceTableNames(context.Context, string) ([]string, error)
	GetKeyspaceTableSchema(context.Context, PlanetScaleSource, string, string) (map[string]*Column, error)
	GetKeyspaceTablePrimaryKeys(context.Context, string, string) ([]string, error)
	GetKeyspaces(context.Context, PlanetScaleSource) ([]string, error)
	GetVitessShards(ctx context.Context, psc PlanetScaleSource) ([]string, error)
	Close() error
}

func NewMySQL(psc *PlanetScaleSource) (PlanetScaleEdgeMysqlAccess, error) {
	db, err := sql.Open("mysql", psc.DSN(psdbconnect.TabletType_primary))
	if err != nil {
		return nil, err
	}

	return planetScaleEdgeMySQLAccess{
		db: db,
	}, nil
}

type planetScaleEdgeMySQLAccess struct {
	db *sql.DB
}

func (p planetScaleEdgeMySQLAccess) Close() error {
	return p.db.Close()
}

func (p planetScaleEdgeMySQLAccess) GetVitessShards(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
	var shards []string

	// TODO: is there a prepared statement equivalent?
	shardNamesQR, err := p.db.QueryContext(
		ctx,
		`show vitess_shards like "%`+psc.Database+`%";`,
	)
	if err != nil {
		return shards, errors.Wrap(err, "Unable to query database for shards")
	}

	for shardNamesQR.Next() {
		var name string
		if err = shardNamesQR.Scan(&name); err != nil {
			return shards, errors.Wrap(err, "unable to get shard names")
		}

		shards = append(shards, strings.TrimPrefix(name, psc.Database+"/"))
	}

	if err := shardNamesQR.Err(); err != nil {
		return shards, errors.Wrapf(err, "unable to iterate shard names for %s", psc.Database)
	}
	return shards, nil
}

func (p planetScaleEdgeMySQLAccess) PingContext(ctx context.Context, psc PlanetScaleSource) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return p.db.PingContext(ctx)
}

func (p planetScaleEdgeMySQLAccess) GetKeyspaceTableNames(ctx context.Context, keyspaceName string) ([]string, error) {
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

func (p planetScaleEdgeMySQLAccess) GetKeyspaceTableSchema(ctx context.Context, psc PlanetScaleSource, tableName string, keyspaceName string) (map[string]*Column, error) {
	properties := map[string]*Column{}

	columnNamesQR, err := p.db.QueryContext(
		ctx,
		"select column_name, column_type from information_schema.columns where table_name=? AND table_schema=?;",
		tableName, keyspaceName,
	)
	if err != nil {
		return properties, errors.Wrapf(err, "Unable to get column names & types for table %v in keyspace %s", tableName, keyspaceName)
	}

	for columnNamesQR.Next() {
		var (
			name       string
			columnType string
		)
		if err = columnNamesQR.Scan(&name, &columnType); err != nil {
			return properties, errors.Wrapf(err, "Unable to scan row for column names & types of table %v in keyspace %s", tableName, keyspaceName)
		}

		properties[name] = &Column{
			Type: getFivetranDataType(columnType, !psc.DoNotTreatTinyIntAsBoolean),
		}
	}

	if err := columnNamesQR.Err(); err != nil {
		return properties, errors.Wrapf(err, "unable to iterate columns for table %s in keyspace %s", tableName, keyspaceName)
	}

	return properties, nil
}

func (p planetScaleEdgeMySQLAccess) GetKeyspaceTablePrimaryKeys(ctx context.Context, tableName string, keyspaceName string) ([]string, error) {
	var primaryKeys []string

	primaryKeysQR, err := p.db.QueryContext(
		ctx,
		"select column_name from information_schema.columns where table_schema=? AND table_name=? AND column_key='PRI';",
		keyspaceName, tableName,
	)
	if err != nil {
		return primaryKeys, errors.Wrapf(err, "Unable to scan row for primary keys of table %v", tableName)
	}

	for primaryKeysQR.Next() {
		var name string
		if err = primaryKeysQR.Scan(&name); err != nil {
			return primaryKeys, errors.Wrapf(err, "Unable to scan row for primary keys of table %v", tableName)
		}

		primaryKeys = append(primaryKeys, name)
	}

	if err := primaryKeysQR.Err(); err != nil {
		return primaryKeys, errors.Wrapf(err, "unable to iterate primary keys for table %s", tableName)
	}

	return primaryKeys, nil
}

func (p planetScaleEdgeMySQLAccess) GetKeyspaces(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
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
