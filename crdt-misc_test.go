package crdt

import "testing"

func TestVersionVector(t *testing.T) {
	assertEq := func(a, b VersionVector) bool {
		if len(a) != len(b) {
			t.Errorf("different length: %d != %d", len(a), len(b))
			t.FailNow()
			return false
		}
		ok := true
		for i := range a {
			if a[i] != b[i] {
				t.Errorf("actorId %d version mismatch: %v != %v", i, a[i], b[i])
			}
		}
		if !ok {
			t.FailNow()
		}
		return ok
	}
	A, B := VersionVector{1, 1, 0, 4}, VersionVector{2, 0, 3, 0}
	A.Merge(B)
	assertEq(A, VersionVector{2, 1, 3, 4})
	B.Merge(A)
	assertEq(B, VersionVector{2, 1, 3, 4})
}
