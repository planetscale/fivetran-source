# fivetran-source
PlanetScale Source connector for FiveTran


## Using grpcurl
Install grpcurl following [these instructions](https://github.com/fullstorydev/grpcurl#installation)

Then, start the server:
```bash
make server
```

From the directory with the sources for this repository (for example, [fivetran_sdk](https://github.com/fivetran/fivetran_sdk)), run:
``` bash
 grpcurl -proto connector_sdk.proto  -import-path ./proto -plaintext 127.0.0.1:50051 fivetran_sdk.Connector.ConfigurationForm
```

To test (on a PlanetScale database):
```bash
grpcurl -proto connector_sdk.proto -import-path . -plaintext -d '{"name": "check_connection", "configuration": {"host": "aws.connect.psdb.cloud","database": "my-database","username": "my-username", "password": "my-password"}}' 127.0.0.1:50051 fivetran_sdk.Connector.Test
```

To check schema:
```bash
grpcurl -proto connector_sdk.proto -import-path . -plaintext -d '{"configuration": {"host": "aws.connect.psdb.cloud","database": "my-database","username": "my-username", "password": "my-password"}}' 127.0.0.1:50051 fivetran_sdk.Connector.Schema
```

To test syncing:
```bash
grpcurl -proto connector_sdk.proto -import-path . -plaintext -d '{"selection": {"with_schema": {"include_new_schemas": true, "schemas": [{"included": true, "schema_name": "my-database", "include_new_tables": true, "tables": [{"included": true, "table_name": "my-table", "columns": {"column-1": true, "column-2": true}, "include_new_columns": true}]}]}}, "configuration": {"host": "aws.connect.psdb.cloud","database": "my-database","username": "my-username", "password": "my-password"}}' 127.0.0.1:50051 fivetran_sdk.Connector.Update
```