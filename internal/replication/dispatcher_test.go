package replication

import (
	"errors"
	"testing"
	"time"

	"example.com/backupmesh/internal/audit"
	"example.com/backupmesh/internal/journal"
	"example.com/backupmesh/internal/model"
)

func fixedClock(now time.Time) func() time.Time {
	return func() time.Time { return now }
}

func eligibleRegistry(capacity int64) *Registry {
	registry := NewRegistry()
	registry.Register(Destination{ID: "dest-a", Region: "us-east", Capacity: capacity, Healthy: true})
	return registry
}

func planableSnapshot(id string, data []byte) model.Snapshot {
	return model.Snapshot{
		ID:         id,
		Generation: 1,
		State:      model.SnapshotStaged,
		Chunks:     []model.Chunk{{Index: 1, Digest: "digest-1", Data: data}},
	}
}

// TestPlanSnapshotReleasesBudgetOnJournalFailure reproduces the ghost-reservation
// bug: when journaling the plan fails (e.g. the journal is closed), the budget
// reserved moments earlier must be returned in full so BudgetUsed is not left
// inflated by the snapshot's byte count.
func TestPlanSnapshotReleasesBudgetOnJournalFailure(t *testing.T) {
	now := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	data := make([]byte, 256)
	total := int64(len(data))

	budget := NewBudget(total)
	registry := eligibleRegistry(total)
	journalLog := journal.New()
	ledger := audit.New()
	dispatcher := NewDispatcher(registry, budget, journalLog, ledger, fixedClock(now))

	snapshot := planableSnapshot("snap-1", data)

	journalLog.Close()
	_, err := dispatcher.PlanSnapshot(snapshot)
	if !errors.Is(err, model.ErrJournalClosed) {
		t.Fatalf("err = %v, want ErrJournalClosed", err)
	}
	if used := dispatcher.BudgetUsed(); used != 0 {
		t.Fatalf("budget used after failed plan = %d, want 0 (ghost reservation leaked)", used)
	}
	if _, _, reserved := budget.Snapshot(); reserved != 0 {
		t.Fatalf("reserved count after failed plan = %d, want 0", reserved)
	}
}

// TestPlanSnapshotHoldsBudgetOnSuccess contrasts the failure path: a successful
// plan keeps the reservation until Complete releases it.
func TestPlanSnapshotHoldsBudgetOnSuccess(t *testing.T) {
	now := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	data := make([]byte, 256)
	total := int64(len(data))

	budget := NewBudget(total)
	registry := eligibleRegistry(total)
	journalLog := journal.New()
	ledger := audit.New()
	dispatcher := NewDispatcher(registry, budget, journalLog, ledger, fixedClock(now))

	snapshot := planableSnapshot("snap-2", data)

	plan, err := dispatcher.PlanSnapshot(snapshot)
	if err != nil {
		t.Fatalf("PlanSnapshot: %v", err)
	}
	if used := dispatcher.BudgetUsed(); used != total {
		t.Fatalf("budget used after successful plan = %d, want %d", used, total)
	}
	dispatcher.Complete(plan, true)
	if used := dispatcher.BudgetUsed(); used != 0 {
		t.Fatalf("budget used after complete = %d, want 0", used)
	}
}
