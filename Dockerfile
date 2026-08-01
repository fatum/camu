FROM golang:1.25 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -o /camu ./cmd/camu

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 libgcc-s1 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /camu /usr/local/bin/camu
ENTRYPOINT ["camu"]
CMD ["serve"]
