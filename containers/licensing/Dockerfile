FROM alpine:latest
ARG GH_TOKEN
RUN apk update
RUN apk upgrade

RUN apk add ruby go git
RUN gem install license_finder

RUN git config --global credential.helper store
RUN sh -c 'echo "https://planetscale-actions-bot:$GH_TOKEN@github.com" >> ~/.git-credentials'
RUN go env -w GOPRIVATE=github.com/planetscale/*
