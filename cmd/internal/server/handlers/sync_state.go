package handlers

import (
	"encoding/json"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	fivetransdk "github.com/planetscale/fivetran-proto/go"
)

type SyncState struct {
	Keyspaces map[string]KeyspaceState `json:"keyspaces"`
}

type KeyspaceState struct {
	Streams map[string]ShardStates `json:"streams"`
}

type ShardStates struct {
	Shards map[string]*SerializedCursor `json:"shards"`
}

type SerializedCursor struct {
	Cursor string `json:"cursor"`
}

// StateFromRequest unmarshals the stateJson saved in FiveTran
// and turns that into a structure we can use within the connector to
// incrementally sync tables from PlanetScale.
func StateFromRequest(request StatefulRequest, source PlanetScaleSource, shards []string, schemaSelection fivetransdk.Selection_WithSchema) (*SyncState, error) {
	syncState := SyncState{
		Keyspaces: map[string]KeyspaceState{},
	}

	state := request.GetStateJson()
	if state != "" {
		err := json.Unmarshal([]byte(state), &syncState)
		if err != nil {
			return nil, err
		}
	}

	for _, s := range schemaSelection.WithSchema.Schemas {
		if !s.Included {
			continue
		}
		keyspaceState, ok := syncState.Keyspaces[s.SchemaName]
		if !ok {
			keyspaceState = KeyspaceState{
				Streams: map[string]ShardStates{},
			}
		}
		for _, t := range s.Tables {
			if !t.Included {
				continue
			}
			stateKey := s.SchemaName + ":" + t.TableName
			// if no table cursor was found in the state, or we want to ignore the current cursor,
			// Send along an empty cursor for each shard.
			if _, ok := keyspaceState.Streams[stateKey]; !ok {
				initialState, err := source.GetInitialState(s.SchemaName, shards)
				if err != nil {
					return &syncState, err
				}
				keyspaceState.Streams[stateKey] = initialState
			}
			streamState := keyspaceState.Streams[stateKey]
			// does this streamState have values for all shards?
			// if not, fill them in with initial state.
			for _, shard := range shards {
				if _, ok := streamState.Shards[shard]; !ok {
					streamState.Shards[shard], _ = TableCursorToSerializedCursor(&psdbconnect.TableCursor{
						Shard:    shard,
						Keyspace: s.SchemaName,
						Position: "",
					})
				}
			}
		}
		syncState.Keyspaces[s.SchemaName] = keyspaceState
	}

	return &syncState, nil
}
