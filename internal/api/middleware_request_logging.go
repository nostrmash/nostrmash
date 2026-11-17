package api

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/nostrmash/nostrmash/internal/logging"
	"github.com/nostrmash/nostrmash/internal/metrics"
	"github.com/nostrmash/nostrmash/internal/store/failure"
	"github.com/nostrmash/nostrmash/internal/traceutil"
)

const defaultRouteTemplate = "/_unmatched"

// WithRequestID attaches a request id to the context and response.
func WithRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if id == "" {
			var err error
			id, err = randomID()
			if err != nil {
				id = "unknown"
			}
		}
		ctx := logging.ContextWithRequestID(r.Context(), id)
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// LogRequests emits one structured log line per request.
func LogRequests(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := traceutil.ExtractHTTPContext(r.Context(), r.Header)
		spanCtx, span := traceutil.StartSpan(ctx, "http.request",
			traceutil.KV("http.method", r.Method),
		)
		r = r.WithContext(spanCtx)
		if traceID := traceutil.TraceID(spanCtx); traceID != "" {
			w.Header().Set("X-Trace-ID", traceID)
		}

		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		start := time.Now()
		defer func() {
			if recovered := recover(); recovered != nil {
				span.End(failure.FromPanic(recovered))
				panic(recovered)
			}
			pathTemplate := requestPathTemplate(r)
			span.SetAttr("http.route", pathTemplate)
			var spanErr error
			if sw.code >= http.StatusInternalServerError {
				spanErr = errors.New("http status " + strings.TrimSpace(http.StatusText(sw.code)))
			}
			span.End(spanErr)
			metrics.ObserveAPI(r.Method, pathTemplate, sw.code, time.Since(start))
			logging.WithRequestID(r.Context(), log).Info("http_request",
				"method", r.Method,
				"path_actual", r.URL.Path,
				"path_template", pathTemplate,
				"status", sw.code,
				"duration_ms", time.Since(start).Milliseconds(),
			)
		}()
		next.ServeHTTP(sw, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("response writer does not support hijacking")
	}
	return hijacker.Hijack()
}

func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func randomID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func requestPathTemplate(r *http.Request) string {
	pattern := strings.TrimSpace(r.Pattern)
	if pattern == "" {
		return defaultRouteTemplate
	}
	if parts := strings.SplitN(pattern, " ", 2); len(parts) == 2 {
		pattern = strings.TrimSpace(parts[1])
	}
	if pattern == "" {
		return defaultRouteTemplate
	}
	return pattern
}

// cloudflareWorkersSubrequestIP is the address Cloudflare stamps on
// CF-Connecting-IP for a Worker subrequest that enters another zone (or
// re-enters Cloudflare because the worker fetch is strictly public). Every
// such subrequest shares this one address, so rate limiting on it collapses
// the whole website into a single bucket.
// https://developers.cloudflare.com/fundamentals/reference/http-headers/
var cloudflareWorkersSubrequestIP = net.ParseIP("2a06:98c0:3600::103")

// websiteVisitorIPHeader carries the page visitor when the connecting address
// is the shared Worker address above. A normal client cannot set
// CF-Connecting-IP, and Cloudflare sets CF-Worker to the zone that owns the
// Worker, so the pair is not spoofable from the public edge.
const websiteVisitorIPHeader = "X-NostrMash-Visitor-IP"

// requestClientIP returns the best-effort originating client address.
//
// Behind Coolify/Traefik the API's RemoteAddr is always the proxy, and the
// edge nginx sets X-Real-IP to that proxy address. Website renders call the
// API from a Cloudflare Worker. Same-zone subrequests copy X-Real-IP into
// CF-Connecting-IP. Cross-zone subrequests replace CF-Connecting-IP with the
// shared Worker address, and the worker sends the visitor separately.
//
//  1. Visitor forwarded by our own Worker, when the connecting address is
//     the shared Worker subrequest address and CF-Worker is our zone
//  2. CF-Connecting-IP (visitor, for ordinary edge traffic)
//  3. X-Real-IP (Traefik sets this to the connecting client)
//  4. left-most X-Forwarded-For hop (original client)
//  5. RemoteAddr host
//
// Only syntactically valid IPs are accepted from headers so a garbage or
// spoofed value cannot create unbounded limiter keys.
func requestClientIP(r *http.Request) string {
	cfIP := parseClientIP(r.Header.Get("CF-Connecting-IP"))
	if isCloudflareWorkersSubrequest(cfIP) && isTrustedWebsiteWorker(r.Header.Get("CF-Worker")) {
		if ip := workerForwardedVisitor(r); ip != "" {
			return ip
		}
	}
	if cfIP != "" {
		return cfIP
	}
	if ip := parseClientIP(r.Header.Get("X-Real-IP")); ip != "" {
		return ip
	}
	if ip := firstForwardedIP(r.Header.Get("X-Forwarded-For")); ip != "" {
		return ip
	}
	return parseClientIP(r.RemoteAddr)
}

func isCloudflareWorkersSubrequest(ip string) bool {
	parsed := net.ParseIP(strings.TrimSpace(ip))
	return parsed != nil && cloudflareWorkersSubrequestIP != nil && parsed.Equal(cloudflareWorkersSubrequestIP)
}

func isTrustedWebsiteWorker(zone string) bool {
	switch strings.ToLower(strings.TrimSpace(zone)) {
	case "nostrmash.com":
		return true
	default:
		return false
	}
}

// workerForwardedVisitor reads the visitor our website worker attached.
// The explicit header wins. Otherwise the left-most X-Forwarded-For hop that
// is not the shared Worker address is the visitor the worker already sends.
func workerForwardedVisitor(r *http.Request) string {
	if ip := parseClientIP(r.Header.Get(websiteVisitorIPHeader)); ip != "" && !isCloudflareWorkersSubrequest(ip) {
		return ip
	}
	for _, part := range strings.Split(r.Header.Get("X-Forwarded-For"), ",") {
		ip := parseClientIP(part)
		if ip == "" || isCloudflareWorkersSubrequest(ip) {
			continue
		}
		return ip
	}
	return ""
}

func firstForwardedIP(header string) string {
	for _, part := range strings.Split(header, ",") {
		if ip := parseClientIP(part); ip != "" {
			return ip
		}
	}
	return ""
}

func parseClientIP(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(value); err == nil {
		value = host
	}
	// net.ParseIP rejects bracketed IPv6; strip brackets if present.
	if strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]") {
		value = strings.TrimSuffix(strings.TrimPrefix(value, "["), "]")
	}
	if net.ParseIP(value) == nil {
		return ""
	}
	return value
}
