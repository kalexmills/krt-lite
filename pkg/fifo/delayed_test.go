package fifo_test

import (
	"fmt"
	"github.com/kalexmills/krt-lite/pkg/fifo"
	"sync"
	"testing"
	"time"
)

func TestDelayedQueue(t *testing.T) {
	t.Run("it works with zero latency", func(t *testing.T) {
		testDelay(t, 0, 0)
	})

	t.Run("it works when sends are slow", func(t *testing.T) {
		testDelay(t, 5*time.Millisecond, 0)
	})

	t.Run("it works when receives are slow", func(t *testing.T) {
		testDelay(t, 0, 5*time.Millisecond)
	})
}

func testDelay(t *testing.T, inDelay, outDelay time.Duration) {
	stop := make(chan struct{})
	defer close(stop)

	q := fifo.NewDelayedQueue[int](16)
	lastVal := -1

	q.Run(stop)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for v := range q.Out() {
			if v != lastVal+1 {
				t.Errorf("Unexpected value; expected %d, got %d", lastVal+1, v)
			}
			lastVal = v
			if outDelay > 0 {
				time.Sleep(outDelay)
			}
			fmt.Println("received", v)
		}
		wg.Done()
		fmt.Println("finished reading")
	}()

	for i := 0; i < 100; i++ {
		q.Send(i, inDelay)
	}
	q.Close()
	wg.Wait()

	if lastVal != 99 {
		t.Errorf("Didn't get all values; last received %d", lastVal)
	}
}
