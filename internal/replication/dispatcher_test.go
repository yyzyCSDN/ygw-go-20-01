package replication

import (
	"errors"
	"testing"
	"time"

	"example.com/backupmesh/internal/model"
)

func fixedTime() time.Time { return time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC) }

func mustBuildSnapshot(t *testing.T, id string, generation uint64, data []byte) model.Snapshot {
	t.Helper()
	snap := model.Snapshot{ID: id, Generation: generation, Full: true, State: model.SnapshotStaged}
	chunk := model.Chunk{Index: 0, Digest: "d-" + id, Data: append([]byte(nil), data...)}
	snap.Chunks = []model.Chunk{chunk}
	return snap
}

func newTestDispatcher(budgetLimit int64) (*Dispatcher, *Budget) {
	registry := NewRegistry()
	registry.Register(Destination{ID: "region-east", Region: "east", Capacity: 1 << 20, Healthy: true})
	budget := NewBudget(budgetLimit)
	clock := func() time.Time { return fixedTime() }
	dispatcher := NewDispatcher(registry, budget, nil, nil, clock)
	return dispatcher, budget
}

// TestDuplicatePlanBlocked ensures that planning the same snapshot twice in
// succession does not produce a second in-flight plan: the second attempt is
// rejected and the budget is not charged a second time.
func TestDuplicatePlanBlocked(t *testing.T) {
	dispatcher, budget := newTestDispatcher(1 << 20)
	snap := mustBuildSnapshot(t, "snap-1", 1, []byte("payload"))

	plan1, err := dispatcher.PlanSnapshot(snap)
	if err != nil {
		t.Fatalf("first plan: unexpected error %v", err)
	}
	if used := budget.Used(); used != plan1.TotalBytes {
		t.Fatalf("after first plan: budget used = %d, want %d", used, plan1.TotalBytes)
	}

	// Second consecutive plan for the same snapshot must be rejected.
	plan2, err := dispatcher.PlanSnapshot(snap)
	if err == nil {
		t.Fatalf("second plan: expected error, got plan %s (duplicate in-flight plan)", plan2.ID)
	}
	if !errors.Is(err, model.ErrConflict) {
		t.Fatalf("second plan: error = %v, want ErrConflict", err)
	}

	// Budget must not have been charged a second time.
	if used := budget.Used(); used != plan1.TotalBytes {
		t.Fatalf("after duplicate plan: budget used = %d, want %d (no double charge)", used, plan1.TotalBytes)
	}
}

// TestBudgetReserveIdempotent guards the accounting layer directly: reserving
// the same plan ID twice must not double-count the used bytes.
func TestBudgetReserveIdempotent(t *testing.T) {
	budget := NewBudget(1 << 20)

	if !budget.Reserve("rep-snap-1-1", 100) {
		t.Fatal("first reserve should succeed")
	}
	if budget.Reserve("rep-snap-1-1", 100) {
		t.Fatal("second reserve for same plan id should be rejected")
	}
	if used, _, reserved := budget.Snapshot(); used != 100 || reserved != 1 {
		t.Fatalf("after duplicate reserve: used = %d reserved = %d, want used=100 reserved=1", used, reserved)
	}

	// A different plan id still reserves against the remaining budget.
	if !budget.Reserve("rep-snap-2-1", 50) {
		t.Fatal("reserve for distinct plan id should succeed")
	}
	if used, _, reserved := budget.Snapshot(); used != 150 || reserved != 2 {
		t.Fatalf("after second distinct reserve: used = %d reserved = %d, want used=150 reserved=2", used, reserved)
	}
}
