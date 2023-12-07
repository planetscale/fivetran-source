# fivetran-source
PlanetScale Source connector for FiveTran


## Using grpcurl
1. Install grpcurl following [these instructions](https://github.com/fullstorydev/grpcurl#installation)

2. Then, start the server:
```bash
make server
```

3. Navigate to a directory with the proto sources for this repository (for example, [fivetran_sdk](https://github.com/fivetran/fivetran_sdk))

4. Start testing from the proto sources repository. Execute each of these from the same repository:

4a. Fetch the configuration form:
``` bash
 grpcurl -proto connector_sdk.proto  -import-path ./proto -plaintext 127.0.0.1:50051 fivetran_sdk.Connector.ConfigurationForm
```

4b. Test connecting to your PlanetScale database:
```bash
grpcurl -proto connector_sdk.proto -import-path . -plaintext -d '{"name": "check_connection", "configuration": {"host": "aws.connect.psdb.cloud","database": "my-database","username": "my-username", "password": "my-password"}}' 127.0.0.1:50051 fivetran_sdk.Connector.Test
```

4c. Fetch the schema for your PlanetScale database:
```bash
grpcurl -proto connector_sdk.proto -import-path . -plaintext -d '{"configuration": {"host": "aws.connect.psdb.cloud","database": "my-database","username": "my-username", "password": "my-password"}}' 127.0.0.1:50051 fivetran_sdk.Connector.Schema
```

4d. Simulate copying data from your PlanetScale database:
```bash
grpcurl -proto connector_sdk.proto -import-path . -plaintext -d '{"selection": {"with_schema": {"include_new_schemas": true, "schemas": [{"included": true, "schema_name": "my-database", "include_new_tables": true, "tables": [{"included": true, "table_name": "my-table", "columns": {"column-1": true, "column-2": true}, "include_new_columns": true}]}]}}, "configuration": {"host": "aws.connect.psdb.cloud","database": "my-database","username": "my-username", "password": "my-password"}}' 127.0.0.1:50051 fivetran_sdk.Connector.Update
```

- This will execute an initial copy phase, and maintain the `Update` stream for a while. Any changes (inserts/deletes/updates) to your PlanetScale database during this time should show up in the stream during the subsequent replication phase.