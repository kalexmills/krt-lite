package pkg

import (
	"sync"
)

// An Index allows subsets of items in a Collection to be efficiently retrieved via additional keys.
type Index[T any] interface {
	// Lookup retrieves all objects associated with the given key.
	Lookup(key string) []T
}

// NewNamespaceIndex indexes the provided Collection by namespace.
func NewNamespaceIndex[T GetNamespacer](c IndexableCollection[T]) Index[T] {
	return c.Index(func(t T) []string {
		return []string{t.GetNamespace()}
	})
}

// GetNamespacer is implemented by most runtime.Object.
type GetNamespacer interface {
	GetNamespace() string
}

// mapIndex implements an in-memory index to track groups of items in a collection by key.
type mapIndex[O any] struct {
	parent    Collection[O]
	extractor KeyExtractor[O]

	mut     *sync.RWMutex
	indexed map[string]map[key[O]]struct{}

	// fetchByKeys is used to fetch keys in-bulk from the parent collection. Parent collection must set this.
	fetchByKeys func(map[key[O]]struct{}) []O
}

func newIndex[O any](parent Collection[O], extractor KeyExtractor[O], fetchByKeys func(map[key[O]]struct{}) []O) *mapIndex[O] {
	return &mapIndex[O]{
		parent:  parent,
		indexed: make(map[string]map[key[O]]struct{}),

		mut:         &sync.RWMutex{},
		extractor:   extractor,
		fetchByKeys: fetchByKeys,
	}
}

func (i *mapIndex[O]) handleEvents(events []Event[O]) {

	i.mut.Lock()
	defer i.mut.Unlock()
	for _, ev := range events {
		oKey := getTypedKey(ev.Latest())
		if ev.Old != nil {
			oldIndexKeys := i.extractor(*ev.Old)
			for _, idxKey := range oldIndexKeys {
				if _, ok := i.indexed[idxKey]; ok {
					delete(i.indexed[idxKey], oKey)
				}
			}
		}
		if ev.New != nil {
			newIndexKeys := i.extractor(*ev.New)
			for _, idxKey := range newIndexKeys {
				if _, ok := i.indexed[idxKey]; !ok {
					i.indexed[idxKey] = map[key[O]]struct{}{oKey: {}}
				} else {
					i.indexed[idxKey][oKey] = struct{}{}
				}
			}
		}
	}
}

func (i *mapIndex[O]) Lookup(key string) []O {
	i.mut.RLock()
	defer i.mut.RUnlock()
	oKeys, ok := i.indexed[key]
	if !ok {
		return nil
	}

	return i.fetchByKeys(oKeys)
}

func (i *mapIndex[O]) WaitUntilSynced(stop <-chan struct{}) bool {
	return i.parent.WaitUntilSynced(stop)
}

func (i *mapIndex[O]) HasSynced() bool {
	return i.parent.HasSynced()
}
