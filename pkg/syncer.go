package pkg

import "k8s.io/client-go/tools/cache"

// multiSyncer syncs
type multiSyncer struct {
	syncers []cache.InformerSynced
}

func (c multiSyncer) WaitUntilSynced(stop <-chan struct{}) bool {
	return cache.WaitForCacheSync(stop, c.syncers...)
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
