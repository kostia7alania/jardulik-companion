# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.26.10-alpine@sha256:87c65a688975c0b1d4585d204874c45ca78354670e3a5f11368c5a492303d696 AS build
ARG TARGETOS
ARG TARGETARCH
ENV CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags='-s -w' -o /out/jardulik ./cmd/jardulik

WORKDIR /src/tools/telegram-transfer
COPY tools/telegram-transfer/go.mod tools/telegram-transfer/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY tools/telegram-transfer ./
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags='-s -w' -o /out/telegram-transfer .
RUN mkdir -m 0700 /state && touch /state/.keep

FROM scratch
COPY LICENSE /LICENSE
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/ /app/
COPY --from=build --chown=65532:65532 /state/ /data/
ENV APP_DATA_DIR=/data TELEGRAM_TRANSFER_HELPER=/app/telegram-transfer TMPDIR=/data
USER 65532:65532
ENTRYPOINT ["/app/jardulik"]
CMD ["serve-web"]
