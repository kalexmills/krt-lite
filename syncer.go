package krt_lite

import (
	"context"
	"k8s.io/apimachinery/pkg/util/wait"
	"sync"
	"time"
)

// Syncer is used to indicate that a Collection has synced. A Collection is synced once it has processed
// all of its initial state.
type Syncer interface {
	// WaitUntilSynced blocks until this Collection has synced
	WaitUntilSynced(stopCh <-chan struct{}) bool
	// HasSynced is true if and only if this Collection has synced.
	HasSynced() bool
}

// multiSyncer combines multiple syncers into one.
type multiSyncer struct {
	synced      chan struct{} // synced is used as an optimization to avoid looping
	closeSynced *sync.Once

	syncers []Syncer
}

func newMultiSyncer(syncers ...Syncer) *multiSyncer {
	return &multiSyncer{
		synced:      make(chan struct{}),
		closeSynced: &sync.Once{},
		syncers:     syncers,
	}
}

func (c *multiSyncer) WaitUntilSynced(stop <-chan struct{}) bool {
	select {
	case <-c.synced:
		return true
	default:
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

// pollingSyncer is used to provide us more control over poll intervals than we get from the cache package.
// Only intended for use when an upstream package forces a particular poll interval upon us.
type pollingSyncer struct {
	interval  time.Duration
	hasSynced func() bool
}

func (s *pollingSyncer) WaitUntilSynced(stop <-chan struct{}) bool {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { // convert the stop channel into context cancellation.
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
			return
		}
	}()

	err := wait.PollUntilContextCancel(ctx, s.interval, true,
		func(ctx context.Context) (done bool, err error) {
			if !s.hasSynced() {
				return false, nil
			}
			return true, nil
		},
	)
	return err == nil
}

func (s *pollingSyncer) HasSynced() bool {
	return s.hasSynced()
}
