package fifo

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestQueue(t *testing.T) {
	t.Run("it works with zero latency", func(t *testing.T) {
		test(t, 0, 0)
	})

	t.Run("it works when sends are slow", func(t *testing.T) {
		test(t, 5*time.Millisecond, 0)
	})

	t.Run("it works when receives are slow", func(t *testing.T) {
		test(t, 0, 5*time.Millisecond)
	})
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.In() <- 12
	}
	close(q.In())

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
			case q.In() <- 12:
			}
		}
	}()

	b.ResetTimer()
	var x int
	for i := 0; i < b.N; i++ {
		x = <-q.Out()
	}
	close(stop)

	fmt.Println(x) // avoid compiler optimization

	wg.Wait()
}
