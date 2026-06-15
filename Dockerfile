# syntax=docker/dockerfile:1

ARG GO_VERSION=1.26.4
FROM golang:${GO_VERSION} AS build

WORKDIR /fivetran-source
COPY . .

RUN apt-get update \
    && apt-get install -y --no-install-recommends unzip \
    && rm -rf /var/lib/apt/lists/*
RUN go mod download
RUN make build-server \
    && cp server /connect

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /connect /usr/local/bin/
ENV FIVETRAN_ENTRYPOINT=/usr/local/bin/connect
ENTRYPOINT ["/usr/local/bin/connect"]
