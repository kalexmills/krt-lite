package pkg

import "github.com/kalexmills/krt-plusplus/pkg/fifo"

type processor[T any] struct {
	handler func(o []T)
	queue   *fifo.Queue[[]T]

	syncedCh chan struct{}

	stopCh chan struct{}
}

// newProcessor returns and starts a processor.
func newProcessor[T any](f func(o []T)) *processor[T] {
	result := &processor[T]{
		handler: f,
		queue:   fifo.NewQueue[[]T](1024),
		stopCh:  make(chan struct{}),

		syncedCh: make(chan struct{}),
	}
	result.queue.Run(result.stopCh)
	return result
}

// syncedCh is closed when processor is parentReg.
func (p *processor[T]) syncC() <-chan struct{} {
	return p.syncedCh
}

func (p *processor[T]) stop() {
	close(p.stopCh)
}

func (p *processor[T]) send(os []T, isInInitialList bool) {
	select { // stuck
	case <-p.stopCh:
		return
	case p.queue.In() <- os:
		if isInInitialList {
			close(p.syncedCh)
		}
	}
}

func (p *processor[O]) run() {
	for {
		select { // stuck
		case <-p.stopCh:
			return
		case next, ok := <-p.queue.Out():
			if !ok {
				return
			}
			if len(next) > 0 {
				p.handler(next)
			}
		}
	}
}
