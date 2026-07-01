package scheduler

import (
	"strings"
	"testing"
	"time"

	"github.com/rootwit/rootwit/config"
	rwsync "github.com/rootwit/rootwit/sync"
)

func strptr(s string) *string { return &s }

func TestScheduleInterval(t *testing.T) {
	cases := map[string]time.Duration{
		"*/30 * * * *":  30 * time.Minute,
		"0 * * * *":     time.Hour,
		"*/5 * * * * *": 5 * time.Second, // 6-field (seconds) form
	}
	for expr, want := range cases {
		if got := scheduleInterval(expr); got != want {
			t.Errorf("scheduleInterval(%q) = %s, want %s", expr, got, want)
		}
	}
	if got := scheduleInterval("not a cron"); got != 0 {
		t.Errorf("scheduleInterval on bad input = %s, want 0", got)
	}
}

func TestDetectSyncGaps(t *testing.T) {
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	threshold := time.Hour // 2× a 30-minute schedule

	tables := []config.SyncTableConfig{
		{Name: "fresh"},
		{Name: "stale"},
		{Name: "never"},
	}
	conn := &rwsync.ConnectionState{
		Tables: map[string]*rwsync.TableState{
			// completed 10 min ago — within the window, not a gap
			"fresh": {LastSyncCompleted: strptr(now.Add(-10 * time.Minute).Format(time.RFC3339))},
			// completed 3 hours ago — well past the 1-hour window
			"stale": {LastSyncCompleted: strptr(now.Add(-3 * time.Hour).Format(time.RFC3339))},
			// never completed — skipped (covered by failure alerts / first run)
			"never": {LastSyncCompleted: nil},
		},
	}

	got := detectSyncGaps(conn, tables, threshold, now)
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 stale table, got %d: %v", len(got), got)
	}
	if !strings.Contains(got[0], "stale") {
		t.Errorf("expected the 'stale' table flagged, got %q", got[0])
	}
}

func TestDetectSyncGaps_Guards(t *testing.T) {
	now := time.Now().UTC()
	stale := strptr(now.Add(-5 * time.Hour).Format(time.RFC3339))
	conn := &rwsync.ConnectionState{
		Tables: map[string]*rwsync.TableState{"x": {LastSyncCompleted: stale}},
	}
	tables := []config.SyncTableConfig{{Name: "x"}}

	if r := detectSyncGaps(nil, tables, time.Hour, now); r != nil {
		t.Errorf("nil connection should return nil, got %v", r)
	}
	if r := detectSyncGaps(conn, tables, 0, now); r != nil {
		t.Errorf("zero threshold should disable checks, got %v", r)
	}
	if r := detectSyncGaps(conn, tables, time.Hour, now); len(r) != 1 {
		t.Errorf("stale table should be flagged with a valid threshold, got %v", r)
	}
}
