package pkg

import (
	"sync"
)

type indexer[T any] interface {
	Lookup(key string) []T
}

// index implements an in-memory index which keeps itself up-to-date via an event stream. Lookups are performed by
// calling GetKey for every item returned.
type index[O any] struct {
	parent    Collection[O]
	extractor KeyExtractor[O]

	mut     *sync.RWMutex
	indexed map[string]map[key[O]]struct{}

	// fetchByKeys is used to fetch keys in-bulk from the parent collection. Parent collection must set this.
	fetchByKeys func(map[key[O]]struct{}) []O
}

func newIndex[O any](parent Collection[O], extractor KeyExtractor[O], fetchByKeys func(map[key[O]]struct{}) []O) *index[O] {
	return &index[O]{
		parent:  parent,
		indexed: make(map[string]map[key[O]]struct{}),

		mut:         &sync.RWMutex{},
		extractor:   extractor,
		fetchByKeys: fetchByKeys,
	}
}

func (i *index[O]) handleEvents(events []Event[O]) {
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

func (i *index[O]) Lookup(key string) []O {
	i.mut.RLock()
	defer i.mut.RUnlock()
	oKeys, ok := i.indexed[key]
	if !ok {
		return nil
	}

	return i.fetchByKeys(oKeys)
}
