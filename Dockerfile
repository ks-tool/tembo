FROM golang:1.27-alpine AS builder

WORKDIR /app

COPY . .

ARG VERSION=dev

RUN --mount=type=cache,target=/go/pkg/mod apk add --no-cache make && \
    make build VERSION=${VERSION}


FROM postgres:18-alpine

COPY --from=builder /app/bin/tembo /usr/local/bin/

ENTRYPOINT ["tembo"]
