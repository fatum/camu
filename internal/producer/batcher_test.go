package producer

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBatcher_FlushOnSize(t *testing.T) {
	var flushCount atomic.Int32

	cfg := BatcherConfig{
		MaxSize: 100,
		MaxAge:  10 * time.Second,
		OnFlush: func(partitionID int) error {
			flushCount.Add(1)
			return nil
		},
	}

	b := NewBatcher(cfg)
	defer b.Stop()

	// 10 messages × (20 + 40 overhead) = 600 bytes total — well over MaxSize=100
	for i := 0; i < 10; i++ {
		b.Append(0, int64(20+40))
	}

	time.Sleep(100 * time.Millisecond)

	if flushCount.Load() == 0 {
		t.Fatal("expected at least one flush due to size threshold, got none")
	}
}

func TestBatcher_FlushOnTime(t *testing.T) {
	var flushCount atomic.Int32

	cfg := BatcherConfig{
		MaxSize: 1 << 30, // 1 GB — won't trigger on size
		MaxAge:  50 * time.Millisecond,
		OnFlush: func(partitionID int) error {
			flushCount.Add(1)
			return nil
		},
	}

	b := NewBatcher(cfg)
	defer b.Stop()

	b.Append(0, int64(5+40))

	time.Sleep(200 * time.Millisecond)

	if flushCount.Load() == 0 {
		t.Fatal("expected time-based flush, got none")
	}
}

func TestBatcher_RetainsBufferOnFlushError(t *testing.T) {
	var flushCount atomic.Int32
	fail := atomic.Bool{}
	fail.Store(true)

	b := NewBatcher(BatcherConfig{
		MaxSize: 10,
		MaxAge:  time.Hour,
		OnFlush: func(partitionID int) error {
			flushCount.Add(1)
			if fail.Load() {
				return errors.New("boom")
			}
			return nil
		},
	})
	defer b.Stop()

	if err := b.Append(0, 10); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for flushCount.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if flushCount.Load() != 1 {
		t.Fatalf("expected first flush attempt, got %d", flushCount.Load())
	}

	fail.Store(false)
	if err := b.Flush(0); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if flushCount.Load() != 2 {
		t.Fatalf("expected buffered retry flush, got %d attempts", flushCount.Load())
	}
}

func TestBatcher_SizeFlushDoesNotBlockAppend(t *testing.T) {
	flushStarted := make(chan struct{})
	allowFlush := make(chan struct{})
	var startOnce sync.Once
	b := NewBatcher(BatcherConfig{
		MaxSize: 1,
		MaxAge:  time.Hour,
		OnFlush: func(int) error {
			startOnce.Do(func() { close(flushStarted) })
			<-allowFlush
			return nil
		},
	})
	defer func() {
		close(allowFlush)
		b.Stop()
	}()

	if err := b.Append(0, 1); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	select {
	case <-flushStarted:
	case <-time.After(time.Second):
		t.Fatal("flush did not start")
	}

	done := make(chan error, 1)
	go func() { done <- b.Append(0, 1) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Append blocked on an in-flight flush")
	}
}

func TestBatcher_StopFlushesDataAppendedDuringInflightFlush(t *testing.T) {
	flushStarted := make(chan struct{})
	allowFirstFlush := make(chan struct{})
	var flushes atomic.Int32
	b := NewBatcher(BatcherConfig{
		MaxSize: 1,
		MaxAge:  time.Hour,
		OnFlush: func(int) error {
			if flushes.Add(1) == 1 {
				close(flushStarted)
				<-allowFirstFlush
			}
			return nil
		},
	})

	requireAppend := func() {
		if err := b.Append(0, 1); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	requireAppend()
	select {
	case <-flushStarted:
	case <-time.After(time.Second):
		t.Fatal("first flush did not start")
	}
	requireAppend()

	done := make(chan struct{})
	go func() {
		b.Stop()
		close(done)
	}()
	close(allowFirstFlush)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish")
	}
	if got := flushes.Load(); got != 2 {
		t.Fatalf("flushes = %d, want 2", got)
	}
}

func TestBatcher_BackpressureScalesWithActivePartitions(t *testing.T) {
	b := NewBatcher(BatcherConfig{
		MaxSize:       1 << 30,
		MaxAge:        time.Hour,
		HighWaterMark: 100,
	})
	defer b.Stop()

	// One active partition: allowance is 100.
	if err := b.Append(0, 50); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := b.Append(0, 60); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("Append() error = %v, want ErrBackpressure (50+60 > 100)", err)
	}

	// A second active partition doubles the allowance to 200.
	if err := b.Append(1, 50); err != nil {
		t.Fatalf("Append() second partition error = %v", err)
	}
	if err := b.Append(0, 60); err != nil {
		t.Fatalf("Append() error = %v, want success with 2 partitions", err)
	}
	if err := b.Append(0, 50); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("Append() error = %v, want ErrBackpressure (160+50 > 200)", err)
	}
}

func TestBatcher_BackpressureFirstBatchCannotBypassHWM(t *testing.T) {
	b := NewBatcher(BatcherConfig{
		MaxSize:       1 << 30,
		MaxAge:        time.Hour,
		HighWaterMark: 100,
	})
	defer b.Stop()

	// Before any partition becomes active the allowance must still be at least
	// one partition's HWM, so an oversized first batch is rejected rather than
	// bypassing the bound.
	if err := b.Append(0, 200); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("Append() error = %v, want ErrBackpressure for oversized first batch", err)
	}

	// A batch within the single-partition allowance is accepted.
	if err := b.Append(0, 50); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
}

func TestBatcher_NewPartitionAmongActiveGetsItsOwnAllowance(t *testing.T) {
	b := NewBatcher(BatcherConfig{
		MaxSize:       1 << 30,
		MaxAge:        time.Hour,
		HighWaterMark: 100,
	})
	defer b.Stop()

	// Partition 0 buffers up to its full one-partition allowance.
	if err := b.Append(0, 100); err != nil {
		t.Fatalf("Append(0) error = %v", err)
	}

	// A first append to a new partition 1 must count partition 1 as becoming
	// active, so the allowance is 2*100=200, not 1*100=100. Appending 1 byte
	// (total 101) must be accepted, not spuriously backpressured.
	if err := b.Append(1, 1); err != nil {
		t.Fatalf("Append(1) first byte error = %v, want success (101 <= 200)", err)
	}

	// But exceeding the two-partition allowance must still reject.
	if err := b.Append(1, 100); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("Append(1) overflow error = %v, want ErrBackpressure (201 > 200)", err)
	}
}

func TestBatcher_InFlightFlushStaysInAllowance(t *testing.T) {
	allowFlush := make(chan struct{})
	flushStarted := make(chan struct{})
	var startOnce sync.Once
	b := NewBatcher(BatcherConfig{
		MaxSize:       100,
		MaxAge:        time.Hour,
		HighWaterMark: 100,
		OnFlush: func(int) error {
			startOnce.Do(func() { close(flushStarted) })
			<-allowFlush
			return nil
		},
	})
	defer func() {
		close(allowFlush)
		b.Stop()
	}()

	// Fill partition 0 to its full one-partition allowance and trigger a flush.
	if err := b.Append(0, 100); err != nil {
		t.Fatalf("Append(0) error = %v", err)
	}
	select {
	case <-flushStarted:
	case <-time.After(time.Second):
		t.Fatal("flush did not start")
	}

	// Partition 0's flush is in flight: count is 0 but its 100 bytes are still
	// in totalSize. A first append to a new partition 1 must still be accepted —
	// two active partitions allow 200, and 100+1 <= 200. With the bug (active
	// decremented at drain) the allowance would be 100 and 101 > 100 would
	// spuriously reject this byte.
	if err := b.Append(1, 1); err != nil {
		t.Fatalf("Append(1) error = %v, want success during in-flight flush", err)
	}

	// But exceeding the two-partition allowance (200) must still reject:
	// totalSize would be 100 + 1 + 100 = 201 > 200.
	if err := b.Append(1, 100); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("Append(1) overflow error = %v, want ErrBackpressure (201 > 200)", err)
	}
}

func TestBatcher_BackpressureCountsActiveNotHistoricalPartitions(t *testing.T) {
	b := NewBatcher(BatcherConfig{
		MaxSize:       1 << 30, // never auto-flush
		MaxAge:        time.Hour,
		HighWaterMark: 100,
	})
	defer b.Stop()

	// Touch several partitions and flush them to completion, simulating
	// historical partitions that no longer hold any buffered data.
	for i := 0; i < 5; i++ {
		if err := b.Append(i, 10); err != nil {
			t.Fatalf("Append(%d) error = %v", i, err)
		}
		if err := b.Flush(i); err != nil {
			t.Fatalf("Flush(%d) error = %v", i, err)
		}
	}
	if got := b.totalSize.Load(); got != 0 {
		t.Fatalf("totalSize after flushes = %d, want 0", got)
	}
	if got := b.activePartitions(); got != 0 {
		t.Fatalf("activePartitions after flushes = %d, want 0", got)
	}

	// A single active partition must still be bounded by the per-partition HWM
	// (100), not 5x100 from the historical partitions.
	if err := b.Append(0, 50); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := b.Append(0, 60); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("Append() error = %v, want ErrBackpressure with one active partition", err)
	}
}

func TestBatcher_ConcurrentFirstAppendsNotFalselyRejected(t *testing.T) {
	// Generous HWM so every append is within the bound for a single active
	// partition: 100 appends of 1 byte = totalSize 100, well under 10000. The
	// only way any append fails is the spurious-rejection race where the active
	// count and the target-partition state are read separately: a concurrent
	// goroutine activates the partition between those reads, leaving the
	// observer with active==0 and isActive==true, computing hwm==0 and
	// rejecting a committed produce. The fix computes both under one lock.
	for iter := 0; iter < 50; iter++ {
		b := NewBatcher(BatcherConfig{
			MaxSize:       1 << 30,
			MaxAge:        time.Hour,
			HighWaterMark: 10000,
		})

		const goroutines = 100
		var wg sync.WaitGroup
		errs := make(chan error, goroutines)
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				if err := b.Append(0, 1); err != nil {
					errs <- err
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("concurrent first Append spuriously rejected: %v", err)
		}
		b.Stop()
	}
}
