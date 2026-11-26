package relayintel

import "sync/atomic"

// projectionEnabled gates role-row writes. Canonical replaceable state and
// deletion tombstones stay on. The default is on so existing deployments
// keep projecting until an operator turns the feature off.
var projectionEnabled atomic.Bool

func init() {
	projectionEnabled.Store(true)
}

// SetProjectionEnabled stops role membership projection without stopping
// protocol-correct replaceable-state updates.
func SetProjectionEnabled(enabled bool) {
	projectionEnabled.Store(enabled)
}

// ProjectionEnabled reports whether role rows should be written.
func ProjectionEnabled() bool {
	return projectionEnabled.Load()
}
