package nostr

import "testing"

func TestSupersedesFollowsNIP01(t *testing.T) {
	if !Supersedes(20, "b", 10, "a") {
		t.Fatal("newer created_at must win")
	}
	if Supersedes(10, "b", 10, "a") {
		t.Fatal("the higher event id must lose a timestamp tie")
	}
	if !Supersedes(10, "a", 10, "b") {
		t.Fatal("the lower event id must win a timestamp tie")
	}
	if !Supersedes(10, "a", 10, "a") {
		t.Fatal("replaying the same event must refresh state")
	}
}
