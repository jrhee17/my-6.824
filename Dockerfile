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
RUN go test -v -run 2A -race -count 10 -timeout 200s
RUN go test -v -run TestBasicAgree2B -race -count 10 -timeout 20s
RUN go test -v -run TestRPCBytes2B -race -count 10 -timeout 30s
RUN go test -v -run TestFollowerFailure2B -race -count 10 -timeout 60s
RUN go test -v -run TestLeaderFailure2B -race -count 10 -timeout 60s
RUN go test -v -run TestFailAgree2B -race -count 10 -timeout 60s
RUN go test -v -run TestFailNoAgree2B -race -count 10 -timeout 60s
RUN go test -v -run TestConcurrentStarts2B -race -count 10 -timeout 20s
RUN go test -v -run TestRejoin2B -race -count 10 -timeout 70s
