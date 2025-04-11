package pkg

import (
	"github.com/kalexmills/krt-plusplus/pkg/fifo"
	"sync/atomic"
)

// TODO: rename this registration handler
type processor[T any] struct {
	handler func(o []T)
	queue   *fifo.Queue[[]T]

	isSynced atomic.Bool

	stopCh chan struct{}
}

// newProcessor returns and starts a processor.
func newProcessor[T any](f func(o []T)) *processor[T] {
	result := &processor[T]{
		handler:  f,
		queue:    fifo.NewQueue[[]T](1024),
		stopCh:   make(chan struct{}),
		isSynced: atomic.Bool{},
	}
	result.queue.Run(result.stopCh)
	return result
}

// HasSynced is true when the processor is synced.
func (p *processor[T]) HasSynced() bool {
	return p.isSynced.Load()
}

func (p *processor[T]) stop() {
	close(p.stopCh)
}

func (p *processor[T]) send(os []T, isInInitialList bool) {
	select {
	case <-p.stopCh:
		return
	case p.queue.In() <- os:
	}
}

func (p *processor[O]) run() {
	for {
		select {
		case <-p.stopCh:
			return
		case next, ok := <-p.queue.Out():
			if !ok {
				return
			}
			if len(next) > 0 {
				p.handler(next)
				p.isSynced.Store(true) // TODO: without joins this seems sufficient.
			}
		}
	}
}
