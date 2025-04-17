package pkg

func WithName(name string) CollectorOption {
	return func(m *collectionShared) {
		m.name = name
	}
}

func WithStop(stop <-chan struct{}) CollectorOption {
	return func(m *collectionShared) {
		m.stop = stop
	}
}
