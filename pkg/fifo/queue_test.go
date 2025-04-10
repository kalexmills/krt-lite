package fifo

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestQueueNoPause(t *testing.T) {
	test(t, 0, 0)
}

func TestQueueInDelay(t *testing.T) {
	test(t, 50*time.Millisecond, 0)
}

func TestQueueOutDelay(t *testing.T) {
	test(t, 0, 50*time.Millisecond)
}

func test(t *testing.T, inDelay, outDelay time.Duration) {
	q := NewQueue[int](16)
	lastVal := -1

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
		}
		wg.Done()
		fmt.Println("finished reading")
	}()

	for i := 0; i < 100; i++ {
		q.In() <- i
		if inDelay > 0 {
			time.Sleep(inDelay)
		}
	}
	close(q.In())
	wg.Wait()

	if lastVal != 99 {
		t.Errorf("Didn't get all values; last received %d", lastVal)
	}
}
