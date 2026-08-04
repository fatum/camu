package replication

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/http2"
)

// NewH2CClient creates an HTTP client that speaks h2c (HTTP/2 without TLS).
// A single client should be shared across all internal HTTP communication
// (assignment pushes, failure reports, produce proxying) to multiplex over
// one connection per peer.
func NewH2CClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, addr)
			},
		},
	}
}
