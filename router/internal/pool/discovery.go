package pool

import (
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"time"
)

// ParseEndpoints parses the static worker list format:
//
//	id=http://host:port|sidecarhost:port, id2=...
//
// The "|sidecar" part is optional. Entries may also be separated by newlines
// (WORKERS_FILE); lines starting with # are ignored.
func ParseEndpoints(s string) ([]Endpoint, error) {
	var out []Endpoint
	for _, raw := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == '\n' }) {
		item := strings.TrimSpace(raw)
		if item == "" || strings.HasPrefix(item, "#") {
			continue
		}
		id, rest, ok := strings.Cut(item, "=")
		if !ok || id == "" || rest == "" {
			return nil, fmt.Errorf("pool: bad worker entry %q (want id=url[|sidecar])", item)
		}
		url, sidecar, _ := strings.Cut(rest, "|")
		out = append(out, Endpoint{ID: strings.TrimSpace(id), URL: strings.TrimSpace(url), SidecarAddr: strings.TrimSpace(sidecar)})
	}
	return out, nil
}

// WatchFile re-reads a worker list file every interval and calls update when
// it changes (used by the local experiment harness to add/remove workers).
func WatchFile(ctx context.Context, path string, interval time.Duration, update func([]Endpoint)) {
	last := "\x00"
	for {
		if b, err := os.ReadFile(path); err == nil && string(b) != last {
			if eps, err := ParseEndpoints(string(b)); err == nil {
				last = string(b)
				update(eps)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// WatchDNS resolves a Kubernetes headless Service name every interval. The
// A records are the ready worker pod IPs (the Endpoints object behind the
// Service), so this is membership-from-Endpoints without needing API access.
func WatchDNS(ctx context.Context, name string, httpPort, sidecarPort int, interval time.Duration, update func([]Endpoint)) {
	last := ""
	for {
		ips, err := net.DefaultResolver.LookupHost(ctx, name)
		if err == nil {
			sort.Strings(ips)
			key := strings.Join(ips, ",")
			if key != last {
				last = key
				eps := make([]Endpoint, 0, len(ips))
				for _, ip := range ips {
					ep := Endpoint{ID: ip, URL: fmt.Sprintf("http://%s", net.JoinHostPort(ip, fmt.Sprint(httpPort)))}
					if sidecarPort > 0 {
						ep.SidecarAddr = net.JoinHostPort(ip, fmt.Sprint(sidecarPort))
					}
					eps = append(eps, ep)
				}
				update(eps)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}
