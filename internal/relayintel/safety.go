package relayintel

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/nostrmash/nostrmash/internal/relayurl"
)

// NormalizeBlossomURL canonicalizes an HTTP media server URL.
// Paths are preserved. A trailing slash and a bare root path are the same server.
func NormalizeBlossomURL(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("parse")
	}
	if parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("invalid blossom url")
	}
	switch strings.ToLower(parsed.Scheme) {
	case "https", "http":
	default:
		return "", fmt.Errorf("blossom scheme must be http or https")
	}
	path := strings.TrimSpace(parsed.EscapedPath())
	if path == "/" {
		path = ""
	}
	return strings.ToLower(parsed.Scheme) + "://" + strings.ToLower(parsed.Host) + path, nil
}

// BlossomPublic reports whether a normalized blossom URL is safe to publish
// in global statistics.
func BlossomPublic(normalized string) bool {
	parsed, err := url.Parse(normalized)
	if err != nil {
		return false
	}
	return HostDialable(parsed.Hostname(), false) == nil
}

// HostDialable decides whether this process may open a connection to host.
// Link-local addresses, including the cloud metadata address, are always
// rejected. Private and loopback addresses are rejected unless the deployment
// explicitly opted into them. A DNS name is accepted here; the dialer still
// has to check every address it resolves.
func HostDialable(hostname string, allowPrivate bool) error {
	host := strings.TrimSpace(strings.ToLower(hostname))
	if host == "" {
		return fmt.Errorf("hostname is required")
	}
	host = strings.TrimSuffix(host, ".")
	if isMetadataName(host) {
		return fmt.Errorf("metadata host is not allowed")
	}
	if !allowPrivate {
		if err := relayurl.HostIsPublic(host); err != nil {
			return err
		}
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
			return fmt.Errorf("link-local or unspecified address is not allowed")
		}
		if !allowPrivate && (ip.IsLoopback() || ip.IsPrivate()) {
			return fmt.Errorf("private address is not allowed")
		}
	}
	return nil
}

func isMetadataName(host string) bool {
	switch host {
	case "metadata.google.internal", "metadata.goog", "metadata.internal", "instance-data":
		return true
	default:
		return strings.HasSuffix(host, ".metadata.google.internal")
	}
}

// DialPinned resolves host and connects to an address that passed HostDialable.
// The TCP connection uses the checked IP, so a later DNS answer cannot move it.
func DialPinned(ctx context.Context, network, addr string, allowPrivate bool, timeout time.Duration) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ip, err := resolveDialable(ctx, host, allowPrivate)
	if err != nil {
		return nil, err
	}
	dialer := net.Dialer{Timeout: timeout}
	return dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
}

func resolveDialable(ctx context.Context, host string, allowPrivate bool) (net.IP, error) {
	if err := HostDialable(host, allowPrivate); err != nil {
		return nil, err
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip, nil
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	var fallback error
	for _, candidate := range ips {
		if err := HostDialable(candidate.IP.String(), allowPrivate); err != nil {
			fallback = err
			continue
		}
		return candidate.IP, nil
	}
	if fallback != nil {
		return nil, fallback
	}
	return nil, fmt.Errorf("no address for host")
}
