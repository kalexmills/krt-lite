package fifo

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

// DelayedQueue implements a delayed FIFO queue, where items are scheduled for delivery after a fixed delay.
type DelayedQueue[T any] struct {
	ready   *Queue[T]
	waiting *timedQueue[T]

	running       bool
	in            chan *entry[T]
	timer         *time.Timer
	untilNotEmpty *sync.Cond
}

// NewDelayedQueue returns a new queue with a ring buffer pre-allocated to the provided size.
func NewDelayedQueue[T any](size int) *DelayedQueue[T] {
	res := &DelayedQueue[T]{
		in:    make(chan *entry[T]),
		ready: NewQueue[T](size),
		waiting: &timedQueue[T]{
			queue: make([]*entry[T], 0, size),
		},
		timer:         time.NewTimer(0),
		untilNotEmpty: sync.NewCond(&sync.Mutex{}),
	}
	return res
}

// Run starts this DelayedQueue and returns. If the queue has already started, calling Run is a no-op.
func (d *DelayedQueue[T]) Run(stop <-chan struct{}) {
	if d.running {
		return
	}
	d.running = true

	go d.ready.Run(stop)

	go func() {
		defer close(d.ready.In())

		toWait := func() time.Duration {
			toWait := d.waiting.NextTime().Sub(time.Now())
			if toWait <= 0 {
				toWait = 0
			}
			return toWait
		}

	loop:
		for d.waiting.Len() > 0 || d.in != nil {
			select {
			case <-stop:
				break loop

			case <-d.timer.C:
				for next := d.waiting.NextTime(); next != nil && atOrBefore(*next, time.Now()); next = d.waiting.NextTime() {
					item := heap.Pop(d.waiting)
					d.ready.In() <- item.(*entry[T]).val
				}

				if d.waiting.Len() == 0 {
					if d.in == nil { // if input channel is closed, we are done.
						fmt.Println("input closed; channel empty; life is good")
						return
					}
					fmt.Println("waiting for an hour")
					d.timer.Reset(time.Hour) // wait some ungodly amount of time
				} else {
					fmt.Println("resetting until next time will fire in", toWait().Seconds(), "seconds")
					d.timer.Reset(toWait())
				}

			case nextEntry, ok := <-d.in:
				if !ok {
					// disable this case and finish processing any remaining items.
					d.in = nil
					if d.waiting.Len() == 0 {
						fmt.Println("ending")
						return
					}
					fmt.Println("input channel closed; items still remaining:", d.waiting.Len())
					continue
				}

				heap.Push(d.waiting, nextEntry)

				fmt.Println("pushed value, resetting timer; will fire in", toWait().Seconds(), "seconds")
				d.timer.Reset(toWait())
			}
		}
	}()
}

// Send schedules an item for delivery after the given delay.
func (d *DelayedQueue[T]) Send(item T, delay time.Duration) {
	d.in <- &entry[T]{
		val: item,
		at:  time.Now().Add(delay),
	}
}

// Close closes this queue. Any call to Send which happens after Close will panic.
func (d *DelayedQueue[T]) Close() {
	d.timer.Stop()
	close(d.in)
}

// Out returns the output channel. Items are delivered to this channel in the order they were scheduled.
func (d *DelayedQueue[T]) Out() <-chan T {
	return d.ready.Out()
}

// timedQueue stores entries sorted by time. It implements heap.Interface.
type timedQueue[T any] struct {
	queue []*entry[T]
}

type entry[T any] struct {
	val   T
	at    time.Time
	index int
}

// NextTime returns the time at which the next item will be delivered.
func (tq *timedQueue[T]) NextTime() *time.Time {
	if len(tq.queue) == 0 {
		return nil
	}
	res := tq.queue[0].at
	return &res
}

// Peek returns the next item to be delivered.
func (tq *timedQueue[T]) Peek() *T {
	if len(tq.queue) == 0 {
		return nil
	}
	res := tq.queue[0].val
	return &res
}

func (tq *timedQueue[T]) Len() int { //nolint:unused // implements heap.Interface
	return len(tq.queue)
}

func (tq *timedQueue[T]) Less(i, j int) bool { //nolint:unused // implements heap.Interface
	return tq.queue[i].at.Before(tq.queue[j].at)
}

func (tq *timedQueue[T]) Swap(i, j int) { //nolint:unused // implements heap.Interface
	tq.queue[i], tq.queue[j] = tq.queue[j], tq.queue[i]
}

func (tq *timedQueue[T]) Push(x any) { //nolint:unused // implements heap.Interface
	n := len(tq.queue)
	item := x.(*entry[T])
	item.index = n
	tq.queue = append(tq.queue, item)
}

func (tq *timedQueue[T]) Pop() any { //nolint:unused // implements heap.Interface
	old := tq.queue
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // don't stop the GC from reclaiming the item eventually
	item.index = -1 // for safety
	tq.queue = old[0 : n-1]
	return item
}

func atOrBefore(test, now time.Time) bool {
	return test.Before(now) || test.Equal(now)
}
