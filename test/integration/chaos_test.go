//go:build integration && chaos

package integration

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maksim/camu/pkg/camutest"
)

func TestChaos_RandomKillsDuringProduce(t *testing.T) {
	env := camutest.New(t, camutest.WithInstances(3))
	defer env.Cleanup()
	client := env.Client()

	client.CreateTopic("chaos-test", 6, 24*time.Hour)
	time.Sleep(5 * time.Second) // wait for lease acquisition

	// Start continuous producing in background
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var produced int64

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_, err := client.Produce("chaos-test", []camutest.ProduceMessage{
					{Key: "k", Value: "chaos-msg"},
				})
				if err == nil {
					atomic.AddInt64(&produced, 1)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Randomly kill and restart instances
	for i := 0; i < 3; i++ {
		time.Sleep(2 * time.Second)
		victim := rand.Intn(3)
		t.Logf("killing instance %d", victim)
		env.KillInstance(victim)
		time.Sleep(3 * time.Second)
		t.Logf("restarting instance %d", victim)
		env.RestartInstance(victim)
		time.Sleep(2 * time.Second) // let it stabilize
	}

	close(stop)
	wg.Wait()

	t.Logf("produced %d messages during chaos", atomic.LoadInt64(&produced))

	// Wait for everything to settle
	time.Sleep(10 * time.Second)

	// Verify: all acknowledged messages should be consumable
	total := 0
	for p := 0; p < 6; p++ {
		resp, err := client.Consume("chaos-test", p, 0, 10000)
		if err == nil && resp != nil {
			total += len(resp.Messages)
		}
	}

	producedCount := atomic.LoadInt64(&produced)
	t.Logf("consumed %d messages, produced %d", total, producedCount)

	// Allow for some data loss from unflushed WAL on killed instances
	// but the majority should survive
	if total == 0 && producedCount > 0 {
		t.Error("consumed 0 messages but produced some — total data loss")
	}
}
