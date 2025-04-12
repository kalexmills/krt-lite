package pkg

import (
	"github.com/kalexmills/krt-plusplus/pkg/fifo"
	"sync/atomic"
)

// registrationHandler handles a queue of batched output events for registrations to a derived collection.
type registrationHandler[T any] struct {
	handler func(o []T)
	queue   *fifo.Queue[[]T]

	isSynced atomic.Bool

	stopCh chan struct{}
}

// newRegistrationHandler returns and starts a registrationHandler.
func newRegistrationHandler[T any](f func(o []T)) *registrationHandler[T] {
	result := &registrationHandler[T]{
		handler:  f,
		queue:    fifo.NewQueue[[]T](1024),
		stopCh:   make(chan struct{}),
		isSynced: atomic.Bool{},
	}
	result.queue.Run(result.stopCh)
	return result
}

// HasSynced is true when the registrationHandler is synced.
func (p *registrationHandler[T]) HasSynced() bool {
	return p.isSynced.Load()
}

func (p *registrationHandler[T]) stop() {
	close(p.stopCh)
}

func (p *registrationHandler[T]) send(os []T, isInInitialList bool) {
	select {
	case <-p.stopCh:
		return
	case p.queue.In() <- os:
	}
}

func (p *registrationHandler[O]) run() {
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
				p.isSynced.Store(true) // TODO: this seems sufficient.
			}
		}
	}
}
