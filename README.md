# fivetran-source
PlanetScale Source connector for Fivetran


## Testing locally
### Prerequisites
1. Install grpcurl following [these instructions](https://github.com/fullstorydev/grpcurl#installation)

2. Then, start the server in `fivetran-source` repository:
```bash
make server
```

### Testing the connector
3. Navigate to a directory with the proto sources for this repository (for example, [fivetran_sdk](https://github.com/fivetran/fivetran_sdk))

4. Start testing from the proto sources repository. Execute each of these from the same repository.

5. Fetch the configuration form:
``` bash
 grpcurl -proto connector_sdk.proto  -import-path . -plaintext 127.0.0.1:50051 fivetran_sdk.v2.SourceConnector.ConfigurationForm
```

6. Test connecting to your PlanetScale database:
```bash
grpcurl -proto connector_sdk.proto -import-path . -plaintext -d '{"name": "check_connection", "configuration": {"host": "aws.connect.psdb.cloud","database": "my-database","username": "my-username", "password": "my-password"}}' 127.0.0.1:50051 fivetran_sdk.SourceConnector.Test
```

7. Fetch the schema for your PlanetScale database:
```bash
grpcurl -proto connector_sdk.proto -import-path . -plaintext -d '{"configuration": {"host": "aws.connect.psdb.cloud","database": "my-database","username": "my-username", "password": "my-password"}}' 127.0.0.1:50051 fivetran_sdk.Connector.Schema
```

8. Simulate copying data from your PlanetScale database:
```bash
grpcurl -proto connector_sdk.proto -import-path . -plaintext -d '{"selection": {"with_schema": {"include_new_schemas": true, "schemas": [{"included": true, "schema_name": "my-database", "include_new_tables": true, "tables": [{"included": true, "table_name": "my-table", "columns": {"column-1": true, "column-2": true}, "include_new_columns": true}]}]}}, "configuration": {"host": "aws.connect.psdb.cloud","database": "my-database","username": "my-username", "password": "my-password"}}' 127.0.0.1:50051 fivetran_sdk.Connector.Update
```

- This will execute an initial copy phase, and maintain the `Update` stream for a while. Any changes (inserts/deletes/updates) to your PlanetScale database during this time should show up in the stream during the subsequent replication phase.

### Starting from a specific GTID
You can start replication from a specific GTID _per keyspace shard_, by setting `starting_gtids` in your database configuration:
```json
{
    "host": "<FQDN for your PS database>",
    "database":"<default keyspace name>",
    "username":"<username>",
    "password":"<some password for your database>",
    "starting_gtids": "{\"keyspace\": {\"shard\": \"MySQL56/MYGTID:1-3\"}}"
}
```
When using the FiveTran UI, `starting_gtids` will show up as a field in the configuration form.

**Note:** When `starting_gtids` is specified in the configuration file, _and_ a `--state` file is passed, the `--state` file will always take precedence. This is so incremental sync continues working.

**How to get starting GTIDs**

You can get the latest exectued GTID for every shard by querying your database. 
1. Access your PlanetScale database. One way to do so is to use `pscale shell`.
2. Target the keyspace and shard that you would like the latest GTID for by doing `use keyspace/shard`.
    - i.e. `use my_sharded_keyspace/-10`
    - If your database is _unsharded_, you don't have to target a keyspace or shard. Skip this step.
3. Execute `select @@gtid_executed;`
4. You'll get a result that looks something like:
```
my_sharded_keyspace/-10> select @@gtid_executed\G
*************************** 1. row ***************************
@@`gtid_executed`: 16cec08d-f91b-11ee-8afb-aacaf4984ae5:1-5639808,
b13c2fe0-f91a-11ee-aa81-6251c27c9c24:1-2487289,
fe8e2a3c-f91a-11ee-9812-82f5834c1ba7:1-46602355
1 row in set (0.01 sec)
```
5. Use the GTIDs returned to form the starting GTID for that shard (in this example, shard `-10`):
```
{"my_sharded_keyspace": {"-10": "MySQL56/16cec08d-f91b-11ee-8afb-aacaf4984ae5:1-5639808,b13c2fe0-f91a-11ee-aa81-6251c27c9c24:1-2487289,fe8e2a3c-f91a-11ee-9812-82f5834c1ba7:1-46602355"}}
```
6. Repeat this process for all your shards, if your database is sharded.

**Note**: Remember to prepend the prefix `MySQL56/` onto your starting GTIDs.