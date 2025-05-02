package krt_lite

import "time"

// WithName provides a name to the collection for use in debugging and logging.
func WithName(name string) CollectionOption {
	return func(m *collectionShared) {
		m.name = name
	}
}

// WithStop provides a stop channel which can be closed to shut the collection down.
func WithStop(stop <-chan struct{}) CollectionOption {
	return func(m *collectionShared) {
		m.stop = stop
	}
}

// WithPollInterval configures the poll interval used by Informers. Has no effect for other collections.
func WithPollInterval(interval time.Duration) CollectionOption {
	return func(m *collectionShared) {
		m.pollInterval = &interval
	}
}

// WithSpuriousUpdates configures collections from Map and FlatMap to send update events downstream even if old and new
// objects are identical. Has no effect for other collections.
func WithSpuriousUpdates() CollectionOption {
	return func(m *collectionShared) {
		m.wantSpuriousUpdates = true
	}
}
