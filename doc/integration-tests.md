# Integration Tests

The integration tests run connector paths against a real PlanetScale/Vitess
database. They are gated behind Go's `integration` build tag, run in CI as a
dedicated job, and can also be run locally.

## Credentials

Keep credentials out of git. The tests accept either a JSON config file:

```json
{
  "host": "aws.connect.psdb.cloud",
  "database": "fivetran",
  "username": "user",
  "password": "password",
  "treat_tiny_int_as_boolean": true,
  "use_replica": false
}
```

or environment variables:

```sh
export DATABASE_HOST=aws.connect.psdb.cloud
export DATABASE_NAME=fivetran
export DATABASE_USERNAME=...
export DATABASE_PASSWORD=...
```

Optional environment overrides:

```sh
export DATABASE_TREAT_TINY_INT_AS_BOOLEAN=true
export DATABASE_USE_REPLICA=false
export DATABASE_SHARDED_NAME=fivetran_sharded
```

The GitHub Actions `Integration Tests` job runs `make test-integration-stress`
and reads credentials from these repository secrets:

- `PS_INTEGRATION_DATABASE_HOST`
- `PS_INTEGRATION_DATABASE_NAME`
- `PS_INTEGRATION_DATABASE_USERNAME`
- `PS_INTEGRATION_DATABASE_PASSWORD`

CI expects a sibling sharded keyspace named `${DATABASE_NAME}_sharded`. It must
have two shards so the suite can prove that VStream reads and checkpoints both
shards. For example:

```sh
pscale keyspace create <database> <branch> <database>_sharded \
  --cluster-size ps-10 \
  --shards 2 \
  --wait \
  --org <org>
```

The sharded tests create their own tables in that keyspace and add a temporary
`hash(id)` VSchema vindex for each table before writing rows.

## Running

Using a config file:

```sh
PS_INTEGRATION_CONFIG=/absolute/path/to/integration.json make test-integration
```

Using environment variables:

```sh
make test-integration
```

To include the heavier burst-load scenario:

```sh
make test-integration-stress
```

Stress settings can be tuned with environment variables:

```sh
export INTEGRATION_STRESS_ROWS=1000
export INTEGRATION_STRESS_CYCLES=5
```

The tests create tables with the `fs_integration_` prefix and drop them during
test cleanup. Use an empty disposable database or branch. If a run is
interrupted, manually drop leftover `fs_integration_%` tables before the next
run.

## Coverage

The default suite exercises:

- real MySQL DDL/DML setup
- schema discovery from `information_schema`
- real Connect/VStream reads through `lib.ConnectClient`
- production `Sync.Handle`
- production schema-aware record serialization
- checkpoint round-tripping between sync calls
- insert, update, delete, truncate, and `tinyint(1)` boolean serialization
- `BIT` values across widths
- enum and set schema values that include commas and escaped quotes
- scalar serialization for decimal, JSON, binary, date, datetime, timestamp, and null values
- idle syncs that should emit no records once the cursor is current
- repeated mutation bursts across several checkpointed syncs
- sibling keyspace shard discovery where one keyspace name is a prefix of another
- sharded-keyspace syncs with temporary VSchema entries, rows on both shards, and checkpoint state for every shard

The stress suite repeats the burst pattern with configurable row counts and
cycles. Add future scenarios as additional `TestIntegration...` tests in the
same build-tagged package.
