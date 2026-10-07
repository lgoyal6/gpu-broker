# One image for every role (ADR 0001): `gpubroker <role>`. Base images are
# pinned by digest; bump with scripts/pin-images.sh and review the diff.
FROM golang:1.27.1-alpine@sha256:8a5910f31396cd4d89662f56c68b3ae31d374308270a1c3bd96672ee5ed43414 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/gpubroker ./cmd/gpubroker \
 && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/gpuctl ./cmd/gpuctl

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/gpubroker /out/gpuctl /usr/local/bin/
COPY data /data
WORKDIR /
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/gpubroker"]
