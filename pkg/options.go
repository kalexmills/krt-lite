package pkg

func WithName(name string) CollectorOption {
	return func(m *collectorMeta) {
		m.name = name
	}
}
