package fifo

// Queue implements an unbounded single-producer single-consumer fifo queue which is backed by a circular buffer.
type Queue[T any] struct {
	in  chan T
	out chan T

	queue   *ringbuf[T]
	running bool
}

// NewQueue returns a new queue with a ring buffer pre-allocated to the provided size.
func NewQueue[T any](size int) *Queue[T] {
	return &Queue[T]{
		in:  make(chan T),
		out: make(chan T),

		queue:   newRingBuf[T](size),
		running: false,
	}
}

// Run starts this queue and blocks the current thread. If the queue has already started, calling Run is a no-op.
func (q *Queue[T]) Run(stop <-chan struct{}) {
	if q.running {
		return
	}
	var zero T

	go func() {
		outCh := func() chan<- T {
			if q.queue.Len() == 0 {
				return nil
			}
			return q.out
		}
		curVal := func() T {
			if q.queue.Len() == 0 {
				return zero
			}
			return q.queue.Peek()
		}

	loop:
		for q.queue.Len() > 0 || q.in != nil {
			select {
			case <-stop:
				break loop
			case t, ok := <-q.in:
				if !ok {
					q.in = nil
				} else {
					q.queue.Add(t)
				}
			case outCh() <- curVal():
				q.queue.Remove()
			}
		}
		q.running = false
		close(q.out)
	}()
	q.running = true
}

// In returns the input channel. Closing the input channel closes the queue and shuts down the running goroutine.
func (q *Queue[T]) In() chan<- T {
	return q.in
}

// Out returns the output channel.
func (q *Queue[T]) Out() <-chan T {
	return q.out
}
