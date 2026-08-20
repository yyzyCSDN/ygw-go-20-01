package replication

import (
	"fmt"
	"sync"
	"testing"
)

// TestBudgetConcurrentAccess is a regression test for the data race between
// the unlocked read accessors Used()/Limit() and the locked mutators
// Reserve()/Release(). Before the fix, Used() read b.used without holding
// b.mu while Reserve/Release modified it under the lock, so the race
// detector flagged a concurrent read/write and production occasionally
// observed a budget total that did not add up.
//
// Run with: go test -race -run TestBudgetConcurrentAccess ./internal/replication
func TestBudgetConcurrentAccess(t *testing.T) {
	const limit = 1 << 20 // 1 MiB
	b := NewBudget(limit)

	var wg sync.WaitGroup
	// Writers: reserve, observe, then release each plan.
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			planID := fmt.Sprintf("plan-%d", i)
			if b.Reserve(planID, 1024) {
				_ = b.Used()
				_ = b.Limit()
				b.Release(planID)
			}
		}(i)
	}
	// Readers: hammer the read accessors concurrently with the writers.
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = b.Used()
			_ = b.Limit()
		}()
	}
	wg.Wait()

	// Every reservation was released, so the used total must settle to zero
	// and the immutable limit must be unchanged. A lost update or crossed
	// reservation would leave a non-zero (or negative-clamped) total here.
	if got := b.Used(); got != 0 {
		t.Fatalf("used = %d after balanced reserve/release, want 0 (lost update or crossed data)", got)
	}
	if got := b.Limit(); got != limit {
		t.Fatalf("limit = %d, want %d", got, limit)
	}
}
