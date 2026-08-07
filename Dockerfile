FROM golang:1.25 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /camu ./cmd/camu
RUN CGO_ENABLED=0 go build -o /benchmark-service ./cmd/benchmark-service

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /camu /usr/local/bin/camu
COPY --from=builder /benchmark-service /usr/local/bin/benchmark-service
ENTRYPOINT ["/usr/local/bin/camu"]
CMD ["serve"]
