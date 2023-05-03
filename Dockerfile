# syntax=docker/dockerfile:1

ARG GO_VERSION=1.20.1

FROM golang:${GO_VERSION}-bullseye AS build

ARG GH_TOKEN
WORKDIR /fivetran-source
COPY . .

RUN git config --global credential.helper store
RUN sh -c 'echo "https://planetscale-actions-bot:$GH_TOKEN@github.com" >> ~/.git-credentials'
RUN go env -w GOPRIVATE=github.com/planetscale/*


RUN go mod download
RUN make build-server
RUN cp server /connect

FROM debian:bullseye-slim

RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y default-mysql-client ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build connect /usr/local/bin/
ENV FIVETRAN_ENTRYPOINT "/usr/local/bin/connect"
ENTRYPOINT ["/usr/local/bin/connect"]
