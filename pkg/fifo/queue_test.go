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
	test(t, 5*time.Millisecond, 0)
}

func TestQueueOutDelay(t *testing.T) {
	test(t, 0, 5*time.Millisecond)
}

func test(t *testing.T, inDelay, outDelay time.Duration) {
	stop := make(chan struct{})
	defer close(stop)

	q := NewQueue[int](16)
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

func BenchmarkQueue_Send(b *testing.B) {
	stop := make(chan struct{})
	defer close(stop)

	q := NewQueue[int](1024)
	q.Run(stop)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for out := range q.Out() {
			_ = out
		}
	}()

	for b.Loop() {
		q.In() <- 12
	}
	close(q.In())

	fmt.Println("finished with capacity", q.queue.Cap())
	wg.Wait()
}

func BenchmarkQueue_Receive(b *testing.B) {
	stop := make(chan struct{})

	q := NewQueue[int](1024)
	q.Run(stop)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(q.In())
		for {
			select {
			case <-stop:
				return
			default:
				q.In() <- 12
			}
		}
	}()

	var x int
	for b.Loop() {
		x = <-q.Out()
	}
	close(stop)

	fmt.Println(x)

	fmt.Println("finished with capacity", q.queue.Cap())
	wg.Wait()
}
