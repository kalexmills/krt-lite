package pkg

// Context carries internal signals which allow Fetch to subscribe to upstream collections.
type Context interface {
	registerDependency(d *dependency, s Syncer, register func(func([]Event[any])) Syncer)
}

type kontext[I, O any] struct {
	collection   *derivedCollection[I, O]
	key          key[I]
	dependencies []*dependency
}

func (ctx *kontext[I, O]) registerDependency(d *dependency, syn Syncer, register func(func([]Event[any])) Syncer) {
	// register a watch on the parent collection, unless we already have one
	if _, ok := ctx.collection.collectionDependencies[d.collectionID]; !ok {
		l := ctx.collection.logger().With("fromCollectionName", d.collectionName, "fromCollectionID", d.collectionID)

		l.Debug("registering dependency")

		syn.WaitUntilSynced(ctx.collection.stop)

		ctx.collection.collectionDependencies[d.collectionID] = struct{}{}

		register(func(anys []Event[any]) {
			l.Debug("pushing fetch events", "count", len(anys))
			ctx.collection.pushFetchEvents(d.collectionID, anys)
		}).WaitUntilSynced(ctx.collection.stop)

		l.Debug("dependency registration has synced")
	}
	ctx.dependencies = append(ctx.dependencies, d)
}

// Fetch performs a List operation against the provided collection, subscribing to updates if ctx is set. If ctx is nil,
// this is a one-time operation.
func Fetch[T any](ctx Context, c Collection[T]) []T {
	d := &dependency{
		collectionID:   c.getUID(),
		collectionName: c.getName(),
	}
	if ctx != nil {
		ctx.registerDependency(d, c, func(handler func([]Event[any])) Syncer {
			ff := func(ts []Event[T]) {
				// do type erasure to cast from T to any.
				anys := make([]Event[any], len(ts))
				for i, t := range ts {
					anys[i] = castEvent[T, any](t)
				}

				handler(anys)
			}

			return c.RegisterBatched(ff, false)
		})
	}

	return c.List()
}

type dependency struct {
	collectionID   uint64
	collectionName string
}
