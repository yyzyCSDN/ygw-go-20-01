package replication

import (
	"testing"
	"time"

	"example.com/backupmesh/internal/audit"
	"example.com/backupmesh/internal/journal"
)

func newTestDispatcher(t *testing.T, destinations []Destination, ledger *audit.Ledger) (*Dispatcher, func() time.Time) {
	t.Helper()
	now := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	registry := NewRegistry()
	for _, destination := range destinations {
		if !registry.Register(destination) {
			t.Fatalf("register destination %s", destination.ID)
		}
	}
	if ledger == nil {
		ledger = audit.New()
	}
	dispatcher := NewDispatcher(registry, NewBudget(1<<30), journal.New(), ledger, clock)
	return dispatcher, clock
}

// TestCompleteIsIdempotent guards against the duplicate-completion bug: when
// the same replication plan's completion interface is invoked twice, the
// outcome window, summary statistics, completed count, and audit ledger must
// each reflect a single completion rather than two.
func TestCompleteIsIdempotent(t *testing.T) {
	destinations := []Destination{
		{ID: "region-east", Region: "east", Capacity: 1 << 20, Healthy: true},
		{ID: "region-west", Region: "west", Capacity: 1 << 20, Healthy: true},
	}
	ledger := audit.New()
	dispatcher, _ := newTestDispatcher(t, destinations, ledger)

	plan, err := dispatcher.Plan("snap-1", 1, []ChunkPlan{{Index: 0, Digest: "d0", Bytes: 128}})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	// The same plan is completed twice, as a duplicate client call would do.
	dispatcher.Complete(plan, true)
	dispatcher.Complete(plan, true)

	want := len(plan.Destinations)
	if outcomes := dispatcher.Outcomes(); len(outcomes) != want {
		t.Fatalf("outcomes = %d, want %d (second completion should be ignored)", len(outcomes), want)
	}

	total, succeeded, failed := dispatcher.OutcomeSummary()
	if total != want {
		t.Fatalf("summary total = %d, want %d", total, want)
	}
	if succeeded != want {
		t.Fatalf("summary succeeded = %d, want %d", succeeded, want)
	}
	if failed != 0 {
		t.Fatalf("summary failed = %d, want 0", failed)
	}

	if _, completed := dispatcher.Status(); completed != 1 {
		t.Fatalf("status completed = %d, want 1", completed)
	}

	completionEvents := 0
	for _, event := range ledger.Events() {
		if event.Kind == "replication-completed" && event.ObjectID == plan.SnapshotID {
			completionEvents++
		}
	}
	if completionEvents != 1 {
		t.Fatalf("audit completion events = %d, want 1", completionEvents)
	}
}

// TestCompleteKeepsFirstOutcome ensures that a second completion with a
// different success flag is dropped rather than overwriting or appending; the
// first recorded outcome is the one that stands.
func TestCompleteKeepsFirstOutcome(t *testing.T) {
	destinations := []Destination{
		{ID: "region-east", Region: "east", Capacity: 1 << 20, Healthy: true},
	}
	dispatcher, _ := newTestDispatcher(t, destinations, audit.New())

	plan, err := dispatcher.Plan("snap-2", 1, []ChunkPlan{{Index: 0, Digest: "d0", Bytes: 64}})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	dispatcher.Complete(plan, false)
	dispatcher.Complete(plan, true) // must be ignored

	outcomes := dispatcher.Outcomes()
	if len(outcomes) != 1 {
		t.Fatalf("outcomes = %d, want 1", len(outcomes))
	}
	if outcomes[0].Succeeded {
		t.Fatalf("outcome succeeded = true, want false (first completion must stand)")
	}

	if total, succeeded, failed := dispatcher.OutcomeSummary(); total != 1 || succeeded != 0 || failed != 1 {
		t.Fatalf("summary = (total=%d, succeeded=%d, failed=%d), want (1, 0, 1)", total, succeeded, failed)
	}
}
