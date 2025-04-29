package pkg

import (
	"sync"
	"sync/atomic"
)

// Syncer is used to indicate that a Collection has synced.
type Syncer interface {
	WaitUntilSynced(stopCh <-chan struct{}) bool
	HasSynced() bool
}

// multiSyncer combines multiple syncers into one.
type multiSyncer struct {
	synced      chan struct{} // synced is used as an optimization to avoid looping
	closeSynced *sync.Once

	syncersMut *sync.RWMutex
	syncers    []Syncer
}

func newMultiSyncer(syncers ...Syncer) *multiSyncer {
	return &multiSyncer{
		synced:      make(chan struct{}),
		closeSynced: &sync.Once{},
		syncersMut:  &sync.RWMutex{},
		syncers:     syncers,
	}
}

func (c *multiSyncer) Add(syncer Syncer) {
	c.syncersMut.Lock()
	defer c.syncersMut.Unlock()
	c.syncers = append(c.syncers, syncer)
}

// WaitUntilSynced waits until the currently configured syncers have all completed.
func (c *multiSyncer) WaitUntilSynced(stop <-chan struct{}) bool {
	select {
	case <-c.synced:
		return true
	default:
		c.syncersMut.RLock()
		defer c.syncersMut.RUnlock()
		for _, s := range c.syncers {
			if !s.WaitUntilSynced(stop) {
				return false
			}
		}
		c.closeSynced.Do(func() {
			close(c.synced)
		})
		return true
	}
}

func (c *multiSyncer) HasSynced() bool {
	select {
	case <-c.synced:
		return true
	default:
		c.syncersMut.RLock()
		defer c.syncersMut.RUnlock()
		for _, s := range c.syncers {
			if !s.HasSynced() {
				return false
			}
		}
		c.closeSynced.Do(func() {
			close(c.synced)
		})
		return true
	}
}

type alwaysSynced struct{}

func (s alwaysSynced) WaitUntilSynced(stop <-chan struct{}) bool {
	return true
}

func (s alwaysSynced) HasSynced() bool { return true }

type channelSyncer struct {
	synced <-chan struct{}
}

func (s channelSyncer) WaitUntilSynced(stop <-chan struct{}) bool {
	select {
	case <-s.synced:
		return true
	case <-stop:
		return false
	}
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
type idSyncer struct { //nolint:unused // May be used by Join
	count   *atomic.Int64
	indices *sync.Map
	synced  chan struct{}
}
