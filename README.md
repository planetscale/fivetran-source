# fivetran-source
PlanetScale Source connector for Fivetran


## Using grpcurl
Install grpcurl following [these instructions](https://github.com/fullstorydev/grpcurl#installation)

Then, start the server:
```bash
make server
```

From the directory with the sources for this repository, run:
``` bash
 grpcurl -proto fivetran_sdk.proto  -import-path ./proto -plaintext 127.0.0.1:8000 fivetran_sdk.Connector.ConfigurationForm
```
