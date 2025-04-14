package pkg

import (
	"k8s.io/client-go/tools/cache"
	"sync"
	"sync/atomic"
)

// multiSyncer syncs
type multiSyncer struct {
	syncers []cache.InformerSynced
}

func (c multiSyncer) WaitUntilSynced(stop <-chan struct{}) bool {
	return cache.WaitForCacheSync(stop, c.syncers...) // TODO: this only waits for whatever syncers are currently present
}

func (c multiSyncer) HasSynced() bool {
	for _, s := range c.syncers {
		if !s() {
			return false
		}
	}
	return true
}

type alwaysSynced struct{}

func (s alwaysSynced) HasSynced() bool { return true }

type channelSyncer struct {
	synced <-chan struct{}
}

func (s channelSyncer) HasSynced() bool {
	select {
	case <-s.synced:
		return true
	default:
		return false
	}
}

// idSyncer is a bit like a waitgroup.
type idSyncer struct {
	count   *atomic.Int32
	indices *sync.Map
	synced  chan struct{}
}

// newIDSyncer creates a new ID syncer with IDs 0,...,n-1
func newIDSyncer(n int) *idSyncer {
	if n == 0 {
		ch := make(chan struct{})
		close(ch)
		return &idSyncer{
			synced: ch,
		}
	}

	count := &atomic.Int32{}
	count.Store(int32(n))
	indices := &sync.Map{}
	for i := 0; i < n; i++ {
		indices.Store(int32(i), false)
	}
	return &idSyncer{
		count:   count,
		indices: indices,
		synced:  make(chan struct{}),
	}
}

func (s *idSyncer) WaitUntilSynced(stop <-chan struct{}) bool {
	return cache.WaitForCacheSync(stop, s.HasSynced)
}

func (s *idSyncer) HasSynced() bool {
	select {
	case <-s.synced:

		return true
	default:
		return false
	}
}

func (s *idSyncer) MarkSynced(id int) {
	select {
	case <-s.synced:
		return
	default:
		if s.indices.CompareAndSwap(int32(id), false, true) {
			x := s.count.Add(-1)
			if x == 0 {
				close(s.synced)
				s.indices = nil
			}
		}
	}
}
