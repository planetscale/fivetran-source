# Bugbash Integration Harness

The bugbash harness runs selected connector paths against a real PlanetScale/Vitess
database. It is gated behind the `integration` build tag and is not part of
normal unit tests or CI.

## Credentials

Keep credentials out of git. The harness accepts either a JSON config file:

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
```

## Running

Using a config file:

```sh
PS_BUGBASH_CONFIG=/absolute/path/to/bugbash.json make test-bugbash
```

Using environment variables:

```sh
make test-bugbash
```

The tests create tables with the `fs_bugbash_` prefix and drop them during test
cleanup. Use an empty disposable database or branch. If a run is interrupted,
manually drop leftover `fs_bugbash_%` tables before the next run.

## Current Coverage

`TestBugbashBasicInsertUpdateDelete` exercises:

- real MySQL DDL/DML setup
- schema discovery from `information_schema`
- real Connect/VStream reads through `lib.ConnectClient`
- production `Sync.Handle`
- production schema-aware record serialization
- checkpoint round-tripping between sync calls
- insert, update, delete, truncate, and `tinyint(1)` boolean serialization

Add future scenarios as additional `TestBugbash...` tests with the same build
tag.
