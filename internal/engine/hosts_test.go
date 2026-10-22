package engine

import (
	"context"
	"testing"
	"time"
)

func TestHostBudgetChangesAndCancellation(t *testing.T) {
	hosts := NewHosts(1)
	first, err := hosts.Acquire(context.Background(), "origin")
	if err != nil {
		t.Fatal(err)
	}
	defer first()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		release, err := hosts.Acquire(ctx, "origin")
		if err == nil {
			release()
		}
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatal("cancelled waiter acquired a slot", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter did not cancel")
	}
	hosts.SetLimit(2)
	second, err := hosts.Acquire(context.Background(), "origin")
	if err != nil {
		t.Fatal(err)
	}
	second()
	second() // A repeated release must not manufacture extra capacity.
	third, err := hosts.Acquire(context.Background(), "origin")
	if err != nil {
		t.Fatal(err)
	}
	defer third()
	limited, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if release, err := hosts.Acquire(limited, "origin"); err == nil {
		release()
		t.Fatal("origin exceeded its budget")
	}
	// An unrelated origin retains its own capacity.
	other, err := hosts.Acquire(context.Background(), "other")
	if err != nil {
		t.Fatal(err)
	}
	other()
}
