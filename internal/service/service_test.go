package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/service"
)

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

func TestCaptureVerifyAndRestore(t *testing.T) {
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	svc := service.New(4, fixedClock{now: now})
	handle, err := svc.BeginCapture(context.Background(), "snap-1", "node-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.StageChunk(handle, 1, []byte("alpha")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := svc.CommitCapture(handle, "", true, map[string]string{"tier": "full"})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.State != model.SnapshotPublished {
		t.Fatalf("state = %s", snapshot.State)
	}
	if _, err := svc.VerifySnapshot(snapshot.ID); err != nil {
		t.Fatal(err)
	}
	plan, err := svc.BeginRestore(snapshot.ID, "restore-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	plan = svc.AdvanceRestore(plan, []int{1})
	if plan.Cursor != 1 {
		t.Fatalf("cursor = %d", plan.Cursor)
	}
}

func TestPlanReplicationRejectsExpiredSnapshot(t *testing.T) {
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	svc := service.New(4, fixedClock{now: now})
	handle, err := svc.BeginCapture(context.Background(), "snap-exp", "node-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.StageChunk(handle, 1, []byte("alpha")); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CommitCapture(handle, "", true, nil); err != nil {
		t.Fatal(err)
	}
	// Retention marks the snapshot expired (state flip) without deleting it.
	expired := svc.ApplyRetention(0)
	if len(expired) != 1 || expired[0] != "snap-exp" {
		t.Fatalf("expired = %v", expired)
	}
	// The expired snapshot must not be eligible for a replication plan.
	if _, err := svc.PlanReplication("snap-exp"); !errors.Is(err, model.ErrExpired) {
		t.Fatalf("expected ErrExpired, got err=%v", err)
	}
}
