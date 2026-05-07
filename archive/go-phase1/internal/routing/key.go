// Package routing decides which backend a Spark Connect request should reach.
package routing

// SessionKey identifies a Spark Connect session. The pair (UserID, SessionID)
// is what backend Spark Connect servers themselves use to key SparkSession,
// so the gateway must keep the same routing decision stable across the
// lifetime of that pair.
//
// UserID may be empty if the client did not set it; SessionID must not be
// empty for the affinity store to route a request.
type SessionKey struct {
	UserID    string
	SessionID string
}

// IsZero reports whether the key carries no session identity.
func (k SessionKey) IsZero() bool {
	return k.SessionID == ""
}
