package static

import "testing"

func TestRoundRobin(t *testing.T) {
	p, err := New([]string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	got := []string{p.Pick(), p.Pick(), p.Pick(), p.Pick()}
	want := []string{"a", "b", "c", "a"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("pick %d: got %q want %q", i, got[i], want[i])
		}
	}
}

func TestEmpty(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("expected error on empty pool")
	}
}
