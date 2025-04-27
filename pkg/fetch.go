package pkg

// Context is used to track dependencies between Collections via Fetch.
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

		syn.WaitUntilSynced(ctx.collection.stop) // wait until passed collection is synced.

		ctx.collection.collectionDependencies[d.collectionID] = struct{}{}

		register(func(anys []Event[any]) { // register and wait for sync
			l.Debug("pushing fetch events", "count", len(anys))
			ctx.collection.pushFetchEvents(d.collectionID, anys)
		}).WaitUntilSynced(ctx.collection.stop)

		l.Debug("dependency registration has synced")
	}
	ctx.dependencies = append(ctx.dependencies, d)
}

// Fetch calls List on the provided Collection, and can be used to subscribe Collections created by Map or FlatMap to
// updates from additional collections. Passing a non-nil Context to Fetch subscribes the collection to updates from
// Collection c. When no Context is passed, Fetch is equivalent to c.List().
func Fetch[T any](ktx Context, c Collection[T]) []T {
	if ktx != nil {
		d := &dependency{
			collectionID:   c.getUID(),
			collectionName: c.getName(),
		}

		ktx.registerDependency(d, c, func(handler func([]Event[any])) Syncer {
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
