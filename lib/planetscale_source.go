package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
)

// PlanetScaleSource defines a configured Fivetran Source for a PlanetScale database
// Consider this a connection string to a PlanetScale database.
type PlanetScaleSource struct {
	Host                  string `json:"host"`
	Database              string `json:"database"`
	Username              string `json:"username"`
	Password              string `json:"password"`
	Shards                string `json:"shards"`
	TreatTinyIntAsBoolean bool   `json:"treat_tiny_int_as_boolean"`
	UseReplica            bool   `json:"use_replica"`
	StartingGtids         string `json:"starting_gtids"`
}

// DSN returns a DataSource that mysql libraries can use to connect to a PlanetScale database.
func (psc PlanetScaleSource) DSN(tt psdbconnect.TabletType) string {
	config := mysql.NewConfig()
	config.Net = "tcp"
	config.Addr = psc.Host
	config.User = psc.Username
	config.DBName = psc.Database
	config.Passwd = psc.Password

	if useSecureConnection() {
		config.TLSConfig = "true"
		config.DBName = fmt.Sprintf("%v@%v", psc.Database, tabletTypeToString(tt))
	} else {
		config.TLSConfig = "skip-verify"
	}
	return config.FormatDSN()
}

func tabletTypeToString(t psdbconnect.TabletType) string {
	if t == psdbconnect.TabletType_replica {
		return "replica"
	}

	return "primary"
}

func useSecureConnection() bool {
	e2eTestRun, found := os.LookupEnv("PS_END_TO_END_TEST_RUN")
	if found && (e2eTestRun == "yes" ||
		e2eTestRun == "y" ||
		e2eTestRun == "true" ||
		e2eTestRun == "1") {
		return false
	}

	return true
}

// GetInitialState will return the initial/blank state for a given keyspace in all of its shards.
// This state can be round-tripped safely with Fivetran.
func (psc PlanetScaleSource) GetInitialState(keyspaceOrDatabase string, shards []string) (ShardStates, error) {
	shardCursors := ShardStates{
		Shards: map[string]*SerializedCursor{},
	}

	var startingGtids StartingGtids
	var err error

	if len(psc.Shards) > 0 {
		configuredShards := strings.Split(psc.Shards, ",")
		foundShards := map[string]bool{}
		for _, existingShard := range shards {
			foundShards[existingShard] = true
		}

		for _, configuredShard := range configuredShards {
			if len(configuredShard) > 0 {
				if _, ok := foundShards[strings.TrimSpace(configuredShard)]; !ok {
					return shardCursors, fmt.Errorf("shard %v does not exist on the source database", configuredShard)
				}
			}
		}

		// if we got this far, all the shards that the customer asked for exist in the PlanetScale database.
		shards = configuredShards
	}

	if len(psc.StartingGtids) > 0 {
		startingGtids, err = psc.GetStartingGtids()
		if err != nil {
			return shardCursors, err
		}
	}

	for _, shard := range shards {
		var position string = ""

		// If a starting GTID was specified, use it
		if startingGtids != nil {
			if _, ok := startingGtids[keyspaceOrDatabase]; ok {
				if _, ok := startingGtids[keyspaceOrDatabase][shard]; ok {
					position = startingGtids[keyspaceOrDatabase][shard]
				}
			}
		}

		shardCursors.Shards[shard], _ = TableCursorToSerializedCursor(&psdbconnect.TableCursor{
			Shard:    shard,
			Keyspace: keyspaceOrDatabase,
			Position: position,
		})
	}

	return shardCursors, nil
}

func (psc PlanetScaleSource) GetStartingGtids() (StartingGtids, error) {
	var startingGtids StartingGtids

	if err := json.Unmarshal([]byte(psc.StartingGtids), &startingGtids); err != nil {
		return nil, fmt.Errorf("could not unmarshal starting gtids from string '%s'", psc.StartingGtids)
	}

	return startingGtids, nil
}
