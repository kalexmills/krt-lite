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
	"log"
	"time"
)

// ComparableObject is implemented by pointer-types that implement runtime.Object. For example *corev1.Pod implements
// ComparableObject, while not corev1.Pod does not.
type ComparableObject interface {
	runtime.Object
	comparable
}

const keyIdx = "namespace/name"

func indexByNamespaceName(in any) ([]string, error) {
	obj, err := meta.Accessor(in)
	if err != nil {
		return []string{""}, errors.New("object has no meta")
	}
	key := fmt.Sprintf("%s/%s", obj.GetNamespace(), obj.GetName())
	return []string{key}, nil
}

// NewInformerFromListerWatcher creates a new collection from the provided client
func NewInformerFromListerWatcher[T ComparableObject](lw cache.ListerWatcher) Collection[T] {
	result := informer[T]{
		inf: cache.NewSharedIndexInformer(
			lw,
			zero[T](),
			0,
			cache.Indexers{keyIdx: indexByNamespaceName},
		),
		stop: make(chan struct{}),
	}

	go result.inf.Run(result.stop)

	return result
}

// informer knows how to turn a cache.SharedIndexInformer into a Collection[O].
type informer[T runtime.Object] struct {
	inf  cache.SharedIndexInformer
	stop <-chan struct{}
}

// GetKey retrieves an object by its key. For a Kubernetes informer, this must be the namespace and name of the object.
func (i informer[T]) GetKey(k string) *T {
	obj, exists, err := i.inf.GetIndexer().GetByKey(k)
	if err != nil || !exists {
		return nil
	}
	typed := obj.(T)
	return &typed
}

func (i informer[T]) List() []T {
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

func (i informer[T]) Register(f func(ev Event[T])) cache.ResourceEventHandlerRegistration {
	return i.RegisterBatched(func(events []Event[T]) {
		for _, ev := range events {
			f(ev)
		}
	}, true)
}

func (i informer[T]) RegisterBatched(f func(ev []Event[T]), runExistingState bool) cache.ResourceEventHandlerRegistration {
	registration, err := i.inf.AddEventHandler(eventHandler[T]{
		handler: func(ev Event[T], syncing bool) {
			f([]Event[T]{ev})
		},
	})
	if err != nil {
		panic(err) // TODO: complain differently
	}
	return registration
}

func (i informer[T]) WaitUntilSynced(
	stop <-chan struct{}) (result bool) {
	if i.inf.HasSynced() {
		return true
	}

	t0 := time.Now()

	defer func() {
		log.Printf("synced in %v", time.Since(t0))
	}()

	for {
		select {
		case <-stop:
			return false
		default:
		}
		if i.inf.HasSynced() {
			return true
		}

		// sleep for 1 second, but return if the stop chan is closed.
		t := time.NewTimer(time.Millisecond * 50) // TODO: allow users to set the poll interval
		select {
		case <-stop:
			return false
		case <-t.C:
		}
		log.Printf("waiting for sync for %v", time.Since(t0))
	}
}

func (i informer[T]) HasSynced() bool {
	return i.inf.HasSynced()
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
		// TODO: complain about the cache sending us bad data
		return nil
	}
	o, ok = tombstone.Obj.(T)
	if !ok {
		// TODO: complain again about the cache sending us bad data
		return nil
	}
	return &o
}

// TypedClient is an interface implemented by typed Kubernetes clientsets. TL denotes the type of a list kind, e.g.
// *corev1.PodList
type TypedClient[TL runtime.Object] interface {
	List(ctx context.Context, opts metav1.ListOptions) (TL, error)
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
}

// NewInformer returns a collection backed by an informer which uses the passed client. Caller is responsible to ensure
// that if O denotes a runtime.Object, then TL denotes the corresponding list object. For example, if O is *corev1.Pods,
// TL must be *corev1.PodList.
func NewInformer[T ComparableObject, TL runtime.Object](ctx context.Context, c TypedClient[TL]) Collection[T] {
	return NewInformerFromListerWatcher[T](&cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			return c.List(ctx, opts)
		},
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			return c.Watch(ctx, opts)
		},
	})
}
