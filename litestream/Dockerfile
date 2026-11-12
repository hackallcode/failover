# Litestream built against nats.go with the object store Put fix (nats-io/nats.go#2239) until a release has it.
FROM --platform=$BUILDPLATFORM golang:1.26-bookworm AS build
ARG LITESTREAM=v0.5.18
ARG NATS_GO=29d7657a4627e3dcbd4790b24a066c38534ee68d
ARG TARGETOS
ARG TARGETARCH
RUN git clone --depth 1 --branch ${LITESTREAM} https://github.com/benbjohnson/litestream.git /src
WORKDIR /src
RUN go mod edit -replace github.com/nats-io/nats.go=github.com/hackallcode/nats.go@${NATS_GO} \
    && go mod tidy \
    && CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
       go build -ldflags "-s -w -X main.Version=${LITESTREAM}-natsfix" -o /litestream ./cmd/litestream

FROM scratch
COPY --from=build /litestream /litestream
