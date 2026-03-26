package replication

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPurgatory_WaitCompleted(t *testing.T) {
	p := NewPurgatory()
	defer p.Close()
	done := make(chan error, 1)
	go func() { done <- p.Wait(context.Background(), 500, 5*time.Second) }()
	time.Sleep(10 * time.Millisecond)
	p.Complete(501) // HW=501 means offset 500 is committed
	if err := <-done; err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestPurgatory_WaitTimeout(t *testing.T) {
	p := NewPurgatory()
	defer p.Close()
	err := p.Wait(context.Background(), 500, 50*time.Millisecond)
	if !errors.Is(err, ErrReplicationTimeout) {
		t.Errorf("expected timeout, got: %v", err)
	}
}

func TestPurgatory_MultipleWaiters(t *testing.T) {
	p := NewPurgatory()
	defer p.Close()
	errs := make(chan error, 3)
	for _, o := range []uint64{500, 501, 502} {
		go func(offset uint64) { errs <- p.Wait(context.Background(), offset, 5*time.Second) }(o)
	}
	time.Sleep(10 * time.Millisecond)
	p.Complete(503) // all committed
	for range 3 {
		if err := <-errs; err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
}

func TestPurgatory_WaitContextCancelled(t *testing.T) {
	p := NewPurgatory()
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Wait(ctx, 500, 5*time.Second) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got: %v", err)
	}
}
