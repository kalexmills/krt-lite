package pkg

func WithName(name string) CollectionOption {
	return func(m *collectionShared) {
		m.name = name
	}
}

func WithStop(stop <-chan struct{}) CollectionOption {
	return func(m *collectionShared) {
		m.stop = stop
	}
}
