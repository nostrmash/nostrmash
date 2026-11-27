package nostr

// Supersedes reports whether next should replace current for a NIP-01
// replaceable or addressable event.
//
// A newer created_at wins. Equal timestamps keep the lexicographically
// smaller event id. The same id is a replay and should refresh stored state.
// This is the rule in NIP-01. Callers must use it for both new writes and
// retention so the retained row and the deleted row are not opposites.
func Supersedes(nextCreated int64, nextID string, currentCreated int64, currentID string) bool {
	if nextCreated != currentCreated {
		return nextCreated > currentCreated
	}
	if nextID == currentID {
		return true
	}
	return nextID < currentID
}
