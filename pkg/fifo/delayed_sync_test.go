//go:build goexperiment.synctest

package fifo_test

import (
	"context"
	"github.com/kalexmills/krt-lite/pkg/fifo"
	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestDelayedQueue_DelaysProperly(t *testing.T) {
	// This test uses the synctest experimental module. See https://go.dev/blog/synctest for more about working with
	// synctest.
	synctest.Run(func() {
		ctx, cancel := context.WithCancel(t.Context())

		cint := newCatcher[int]()

		q := fifo.NewDelayedQueue[int](16)
		q.Run(t.Context().Done())

		go cint.Run(ctx, q.Out())

		q.Send(0, time.Second)

		// after almost an entire second, the value should not yet be sent
		time.Sleep(time.Second - time.Nanosecond)
		synctest.Wait()
		assert.Nil(t, cint.Seen())

		q.Send(1, time.Second) // send another value

		time.Sleep(1 * time.Nanosecond)
		synctest.Wait()

		assert.Equal(t, cint.Seen(), ptr.To(0))

		time.Sleep(time.Second - time.Nanosecond) // wait the remainder of the time
		synctest.Wait()

		assert.Equal(t, cint.Seen(), ptr.To(1))

		q.Close()
		cancel()
	})
}

type catcher[T any] struct {
	caught *T
	mut    *sync.Mutex
}

func newCatcher[T any]() *catcher[T] {
	return &catcher[T]{
		mut: &sync.Mutex{},
	}
}

func (c *catcher[T]) Run(ctx context.Context, ch <-chan T) {
	for {
		select {
		case <-ctx.Done():
			return
		case val, ok := <-ch:
			if !ok {
				return
			}
			c.mut.Lock()
			c.caught = &val
			c.mut.Unlock()
		}
	}
}

func (c *catcher[T]) Seen() *T {
	c.mut.Lock()
	defer c.mut.Unlock()
	if c.caught == nil {
		return nil
	}

	res := c.caught
	c.caught = nil
	return res
}
