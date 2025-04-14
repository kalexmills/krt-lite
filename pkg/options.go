package pkg

func WithName(name string) CollectorOption {
	return func(m *collectionMeta) {
		m.name = name
	}
}
