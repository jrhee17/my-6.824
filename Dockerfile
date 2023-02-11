# syntax=docker/dockerfile:1

FROM golang:1.20-alpine

RUN apk update \
    && apk add --no-cache git \
    && apk add --no-cache ca-certificates \
    && apk add --update gcc musl-dev \
    && update-ca-certificates

WORKDIR /app

COPY . .

WORKDIR /app/src/raft
RUN go test -v -run 2A -race
