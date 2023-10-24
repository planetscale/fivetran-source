# syntax=docker/dockerfile:1

ARG GO_VERSION=1.21.3
FROM pscale.dev/wolfi-prod/go:${GO_VERSION} AS build

ARG GH_TOKEN
WORKDIR /fivetran-source
COPY . .

RUN git config --global credential.helper store
RUN sh -c 'echo "https://planetscale-actions-bot:$GH_TOKEN@github.com" >> ~/.git-credentials'
RUN go env -w GOPRIVATE=github.com/planetscale/*

RUN go mod download
RUN make build-server
COPY server /connect

FROM pscale.dev/wolfi-prod/base:latest

COPY --from=build /connect /usr/local/bin/
ENV FIVETRAN_ENTRYPOINT "/usr/local/bin/connect"
ENTRYPOINT ["/usr/local/bin/connect"]
