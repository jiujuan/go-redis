package migration

import "testing"

func TestKeyDistributionTestEdgeCases(t *testing.T) {
	if got := KeyDistributionTest(nil, nil, 10); len(got) != 0 {
		t.Fatalf("nil inputs = %v, want empty map", got)
	}

	dist := KeyDistributionTest([]string{"only"}, []string{"a", "b"}, 1)
	if dist["only"] != 2 {
		t.Fatalf("single node distribution = %v", dist)
	}
}
