mkdir -p fivetran_sdk
protoc \
    --go_out=fivetran_sdk \
    --go_opt=paths=source_relative \
    --go-grpc_out=fivetran_sdk \
    --go-grpc_opt=paths=source_relative \
    common.proto \
    connector_sdk.proto
