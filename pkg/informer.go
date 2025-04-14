package pkg

import (
	"context"
	"errors"
	"fmt"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"log/slog"
	"time"
)

const keyIdx = "namespace/name"

// NewInformer returns a collection backed by an informer which uses the passed client. Caller is responsible to ensure
// that if O denotes a runtime.Object, then TL denotes the corresponding list object. For example, if O is *corev1.Pods,
// TL must be *corev1.PodList.
func NewInformer[T ComparableObject, TL runtime.Object](ctx context.Context, c TypedClient[TL], opts ...CollectorOption) IndexableCollection[T] {
	return NewInformerFromListerWatcher[T](&cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			return c.List(ctx, opts)
		},
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			return c.Watch(ctx, opts)
		},
	}, opts...)
}

// NewInformerFromListerWatcher creates a new collection from the provided cache.ListerWatcher.
func NewInformerFromListerWatcher[T ComparableObject](lw cache.ListerWatcher, opts ...CollectorOption) IndexableCollection[T] {
	i := informer[T]{
		collectionMeta: newCollectorMeta(opts),
		inf: cache.NewSharedIndexInformer(
			lw,
			zero[T](),
			0,
			cache.Indexers{keyIdx: indexByNamespaceName},
		),
		stop:   make(chan struct{}),
		synced: make(chan struct{}),
	}
	i.syncer = &channelSyncer{synced: i.synced}

	go func() {
		cache.WaitForCacheSync(i.stop, i.inf.HasSynced) // TODO: use our own polling wait instead of WaitForCacheSync
		i.logger().Debug("informer has synced")
		close(i.synced)
	}()

	go i.inf.Run(i.stop)

	return &i
}

// informer knows how to turn a cache.SharedIndexInformer into a Collection[O].
type informer[T runtime.Object] struct {
	collectionMeta
	inf    cache.SharedIndexInformer
	stop   chan struct{}
	synced chan struct{}
	syncer *channelSyncer
}

// GetKey retrieves an object by its key. For a Kubernetes informer, this must be the namespace and name of the object.
func (i *informer[T]) GetKey(k string) *T {
	obj, exists, err := i.inf.GetIndexer().GetByKey(k)
	if err != nil || !exists {
		return nil
	}
	typed := obj.(T)
	return &typed
}

func (i *informer[T]) List() []T {
	var res []T
	err := cache.ListAllByNamespace(i.inf.GetIndexer(), metav1.NamespaceAll, labels.Everything(), func(obj any) {
		cast := obj.(T)
		res = append(res, cast)
	})
	if err != nil {
		return nil
	}
	return res
}

func (i *informer[T]) Register(f func(ev Event[T])) Syncer {
	return i.RegisterBatched(func(events []Event[T]) {
		for _, ev := range events {
			f(ev)
		}
	}, true)
}

func (i *informer[T]) RegisterBatched(f func(ev []Event[T]), runExistingState bool) Syncer {
	reg, err := i.inf.AddEventHandler(eventHandler[T]{
		handler: func(ev Event[T], syncing bool) {
			f([]Event[T]{ev})
		},
	})
	if err != nil {
		i.logger().Error("error registering informer event handler", "err", err)
	}

	synced := make(chan struct{})
	go func() {
		cache.WaitForCacheSync(i.stop, reg.HasSynced) // TODO: there's a max of ~100ms on startup to be saved by polling ourselves.
		i.logger().Debug("informer registration has synced")
		close(synced)
	}()

	return newMultiSyncer(i, &channelSyncer{synced: synced})
}

func (i *informer[T]) WaitUntilSynced(stop <-chan struct{}) (result bool) {
	if i.syncer.HasSynced() {
		return true
	}

	t0 := time.Now()

	defer func() {
		i.logger().Info("informer synced", "waitTime", time.Since(t0))
	}()

	for {
		select {
		case <-stop:
			return false
		default:
		}
		if i.syncer.HasSynced() {
			return true
		}

		// sleep for 1 second, but return if the stop chan is closed.
		t := time.NewTimer(time.Millisecond * 100) // TODO: allow users to set the poll interval
		select {
		case <-stop:
			return false
		case <-t.C:
		}
		i.logger().Info("informer waiting for sync", "waitTime", time.Since(t0))
	}
}

func (i *informer[T]) HasSynced() bool {
	return i.syncer.HasSynced()
}

func (i *informer[T]) Index(e KeyExtractor[T]) Index[T] {
	idxKey := fmt.Sprintf("%p", e) // map based on the extractor func.
	err := i.inf.AddIndexers(map[string]cache.IndexFunc{
		idxKey: func(obj any) ([]string, error) {
			t := extract[T](obj)
			if t == nil {
				return nil, nil
			}
			return e(*t), nil
		},
	})
	if err != nil {
		i.logger().Error("failed to add requested indexer", "err", err)
	}
	return &informerIndexer[T]{idxKey: idxKey, inf: i.inf.GetIndexer()}
}

func indexByNamespaceName(in any) ([]string, error) {
	obj, err := meta.Accessor(in)
	if err != nil {
		return []string{""}, errors.New("object has no meta")
	}
	key := fmt.Sprintf("%s/%s", obj.GetNamespace(), obj.GetName())
	return []string{key}, nil
}

type informerIndexer[T any] struct {
	parentName string

	idxKey string
	inf    cache.Indexer
}

func (i *informerIndexer[T]) Lookup(key string) []T {
	res, err := i.inf.ByIndex(i.idxKey, key)
	if err != nil {
		slog.Error("indexer failed to perform key lookup", "err", err, "key", key, "parentName", i.parentName)
	}
	var result []T
	for _, obj := range res {
		result = append(result, obj.(T))
	}
	return result
}

type eventHandler[T runtime.Object] struct {
	handler func(ev Event[T], syncing bool)
}

func (e eventHandler[T]) OnAdd(obj any, isInInitialList bool) {
	e.handler(Event[T]{
		New:   extract[T](obj),
		Event: EventAdd,
	}, isInInitialList)
}

func (e eventHandler[T]) OnUpdate(oldObj, newObj any) {
	e.handler(Event[T]{
		Old:   extract[T](oldObj),
		New:   extract[T](newObj),
		Event: EventUpdate,
	}, false)
}

func (e eventHandler[T]) OnDelete(obj any) {
	e.handler(Event[T]{
		Old:   extract[T](obj),
		Event: EventDelete,
	}, false)
}

func zero[T comparable]() T {
	var z T
	return z
}

func extract[T runtime.Object](obj any) *T {
	if obj == nil {
		return nil
	}
	o, ok := obj.(T)
	if ok {
		return &o
	}
	// check for tombstone from cache
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		slog.Error("failed to extract object of unexpected type", "obj", obj)
		return nil
	}
	o, ok = tombstone.Obj.(T)
	if !ok {
		slog.Error("failed to extract object from tombstone", "objType", fmt.Sprintf("%T", tombstone.Obj), "expectedType", fmt.Sprintf("%T", o))
		return nil
	}
	slog.Error("failed to extract object from type", "actualType", fmt.Sprintf("%T", obj), "expectedTyped", fmt.Sprintf("%T", o))
	return nil
}

// TypedClient is an interface implemented by typed Kubernetes clientsets. TL denotes the type of a list kind, e.g.
// *corev1.PodList
type TypedClient[TL runtime.Object] interface {
	List(ctx context.Context, opts metav1.ListOptions) (TL, error)
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
}
