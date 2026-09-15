package main

import "testing"

func TestRecordingTheSameRoutingKeyTwiceAccumulatesItsCount(t *testing.T) {
	stats := NewStats()
	stats.Record("v1.invoice.changed")
	stats.Record("v1.invoice.changed")

	if got := stats.CountFor("v1.invoice.changed"); got != 2 {
		t.Fatalf("CountFor() = %d, want 2", got)
	}
}

func TestRecordingDistinctRoutingKeysTracksThemSeparately(t *testing.T) {
	stats := NewStats()
	stats.Record("v1.invoice.changed")
	stats.Record("v1.document.changed.contract")

	tests := []struct {
		routingKey string
		want       int64
	}{
		{"v1.invoice.changed", 1},
		{"v1.document.changed.contract", 1},
	}

	for _, tt := range tests {
		if got := stats.CountFor(tt.routingKey); got != tt.want {
			t.Errorf("CountFor(%q) = %d, want %d", tt.routingKey, got, tt.want)
		}
	}
}

func TestTotalSumsEveryRoutingKeyCount(t *testing.T) {
	stats := NewStats()
	stats.Record("v1.invoice.changed")
	stats.Record("v1.document.changed.contract")
	stats.Record("v1.document.changed.contract")

	if got := stats.Total(); got != 3 {
		t.Fatalf("Total() = %d, want 3", got)
	}
}

func TestCountForAnUnseenRoutingKeyIsZero(t *testing.T) {
	stats := NewStats()

	if got := stats.CountFor("v1.user.changed"); got != 0 {
		t.Fatalf("CountFor() = %d, want 0", got)
	}
}
