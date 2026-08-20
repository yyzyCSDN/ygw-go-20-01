package replication

import (
	"errors"
	"testing"
	"time"

	"example.com/backupmesh/internal/audit"
	"example.com/backupmesh/internal/journal"
	"example.com/backupmesh/internal/model"
)

func fixedClock(now time.Time) func() time.Time { return func() time.Time { return now } }

func snapshotWithBytes(id string, bytes int) model.Snapshot {
	return model.Snapshot{
		ID:         id,
		Generation: 1,
		Full:       true,
		State:      model.SnapshotPublished,
		Chunks:     []model.Chunk{{Index: 0, Digest: "d0", Data: make([]byte, bytes)}},
	}
}

func newTestDispatcher(t *testing.T) *Dispatcher {
	t.Helper()
	now := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	registry := NewRegistry()
	budget := NewBudget(1 << 30)
	journalLog := journal.New()
	journalLog.Open()
	ledger := audit.New()
	return NewDispatcher(registry, budget, journalLog, ledger, fixedClock(now))
}

// TestPlanSnapshotRejectsUndersizedDestination reproduces the reported capacity
// bug: a destination registered with capacity 64 must not accept a 200-byte
// snapshot. Planning must reject directly rather than returning a plan whose
// budget reservation cannot be honored.
func TestPlanSnapshotRejectsUndersizedDestination(t *testing.T) {
	d := newTestDispatcher(t)
	d.registry.Register(Destination{ID: "region-east", Region: "east", Capacity: 64, Healthy: true})

	snapshot := snapshotWithBytes("snap-200", 200)
	_, err := d.PlanSnapshot(snapshot)
	if err == nil {
		t.Fatal("expected planning to reject when destination capacity < snapshot bytes, got nil error")
	}
	if !errors.Is(err, model.ErrCapacity) {
		t.Fatalf("expected model.ErrCapacity, got %v", err)
	}

	// A rejected plan must not consume replication budget.
	if used := d.BudgetUsed(); used != 0 {
		t.Fatalf("rejected plan reserved budget: used=%d", used)
	}
}

// TestPlanSnapshotAcceptsAdequateDestination confirms a destination whose
// capacity covers the snapshot still plans successfully.
func TestPlanSnapshotAcceptsAdequateDestination(t *testing.T) {
	d := newTestDispatcher(t)
	d.registry.Register(Destination{ID: "region-east", Region: "east", Capacity: 1 << 20, Healthy: true})

	snapshot := snapshotWithBytes("snap-200", 200)
	plan, err := d.PlanSnapshot(snapshot)
	if err != nil {
		t.Fatalf("expected plan to succeed for adequate capacity, got %v", err)
	}
	if plan.TotalBytes != 200 {
		t.Fatalf("plan total bytes = %d, want 200", plan.TotalBytes)
	}
	if len(plan.Destinations) != 1 || plan.Destinations[0].ID != "region-east" {
		t.Fatalf("unexpected destinations: %+v", plan.Destinations)
	}
}
