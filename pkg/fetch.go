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
	if _, ok := ctx.collection.collectionDependencies[d.id]; !ok {
		syn.WaitUntilSynced(ctx.collection.stop)
		register(func(anys []Event[any]) {
			ctx.collection.pushFetchEvents(d.id, anys)
		}).WaitUntilSynced(ctx.collection.stop)
	}
}

func _apiTest() { // TODO: delete; this is just a reminder of intended usage.
	var C Collection[string]
	var D Collection[float64]
	FlatMap[string, int](C, func(ctx Context, i string) []int {
		floats := Fetch[float64](ctx, D)
		res := make([]int, len(floats))
		for i, f := range floats {
			res[i] = int(f)
		}
		return res
	})
}

// Fetch performs a List operation against the provided collection, subscribing to updates if ctx is set. If ctx is nil,
// this is a one-time operation.
func Fetch[T any](ctx Context, c Collection[T]) []T {
	d := &dependency{
		id:             c.getUID(),
		collectionName: c.getName(),
	}
	if ctx != nil {
		ctx.registerDependency(d, c, func(handler func([]Event[any])) Syncer {
			ff := func(ts []Event[T]) { // do type erasure
				anys := make([]any, len(ts))
				for i, t := range ts {
					anys[i] = any(t)
				}
			}

			return c.RegisterBatched(ff, false)
		})
	}

	return c.List()
}

type dependency struct {
	id             uint64
	collectionName string
}
