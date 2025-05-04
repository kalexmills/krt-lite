package krtlite

import (
	"context"
	"errors"
	"fmt"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"
	"log/slog"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

const keyIdx = "namespace/name"

// NewInformer returns a [Collection] backed by an informer based on the provided [client.WithWatch]. Callers must
// ensure that if T denotes a [runtime.Object], then TL denotes the corresponding list object. For example, if T is
// *corev1.Pods, TL must be corev1.PodList. *TL must implement client.ObjectList.
//
// Objects in this Collection will have keys in the format {namespace}/{name}, or {name} for cluster-scoped objects.
//
// Callers need only specify the first two type arguments. For example:
//
//	Pods := krtlite.NewInformer[*corev1.Pod, corev1.PodList](...)
func NewInformer[T ComparableObject, TL any, PT Ptr[TL]](ctx context.Context, c client.WithWatch, opts ...CollectionOption) IndexableCollection[T] {
	shared := newCollectionShared(opts)
	var listOpts *metav1.ListOptions
	if shared.filter != nil {
		listOpts = shared.filter.ListOptions()
	}

	return NewListerWatcherInformer[T](&cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			if listOpts != nil {
				options.LabelSelector = listOpts.LabelSelector
				options.FieldSelector = listOpts.FieldSelector
			}

			tl := PT(new(TL))
			if err := c.List(ctx, tl, metaOptionsToCtrlOptions(options)...); err != nil {
				return nil, fmt.Errorf("error calling list: %w", err)
			}
			return tl, nil
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			if listOpts != nil {
				options.LabelSelector = listOpts.LabelSelector
				options.FieldSelector = listOpts.FieldSelector
			}

			tl := PT(new(TL))
			wl, err := c.Watch(ctx, tl, metaOptionsToCtrlOptions(options)...)
			if err != nil {
				return nil, fmt.Errorf("error starting watch: %w", err)
			}
			return wl, nil
		},
	}, opts...)
}

// Ptr allows pointers to be instantiated in generic types.
type Ptr[T any] interface {
	*T
	client.ObjectList
}

// NewTypedClientInformer returns a Collection[T] backed by an Informer created from the passed TypedClient. Callers
// must ensure that if T denotes a [runtime.Object], then TL denotes the corresponding list object. For example, if T is
// *corev1.Pods, TL must be corev1.PodList. *TL must implement client.ObjectList.
//
// All objects in this Collection will have keys in the format {namespace}/{name}, or {name} for cluster-scoped objects.
func NewTypedClientInformer[T ComparableObject, TL runtime.Object](ctx context.Context, c TypedClient[TL], opts ...CollectionOption) IndexableCollection[T] {
	shared := newCollectionShared(opts)
	var listOpts *metav1.ListOptions
	if shared.filter != nil {
		listOpts = shared.filter.ListOptions()
	}

	return NewListerWatcherInformer[T](&cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			if listOpts != nil {
				opts.FieldSelector = listOpts.FieldSelector
				opts.LabelSelector = listOpts.LabelSelector
			}
			return c.List(ctx, opts)
		},
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			if listOpts != nil {
				opts.FieldSelector = listOpts.FieldSelector
				opts.LabelSelector = listOpts.LabelSelector
			}
			return c.Watch(ctx, opts)
		},
	}, opts...)
}

// NewListerWatcherInformer creates a new Collection from items returned by the provided [cache.ListerWatcher], which
// must return objects of type T.
//
// Key for objects in this Collection are in the format {namespace}/{name}, or {name} for cluster-scoped objects.
//
// Passing WithFilter to this function has no effect. Filtering must be performed by the passed [cache.ListerWatcher].
func NewListerWatcherInformer[T ComparableObject](lw cache.ListerWatcher, opts ...CollectionOption) IndexableCollection[T] {
	shared := newCollectionShared(opts)

	i := informer[T]{
		collectionShared: shared,
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
	if i.pollInterval == nil {
		i.pollInterval = ptr.To(100 * time.Millisecond)
	}

	go func() {
		syncer := &pollingSyncer{interval: *i.pollInterval, hasSynced: i.inf.HasSynced}
		syncer.WaitUntilSynced(i.stop)
		i.logger().Debug("informer cache has synced")
		close(i.synced)
	}()

	go i.inf.Run(i.stop)

	return &i
}

// InformerFilter provides server-side filters that apply to Informers.
type InformerFilter struct {
	LabelSelector string
	FieldSelector string
	Namespace     string
}

func (f InformerFilter) ListOptions() *metav1.ListOptions {
	opts := metav1.ListOptions{
		LabelSelector: f.LabelSelector,
		FieldSelector: f.FieldSelector,
	}
	if f.Namespace != "" {
		if opts.FieldSelector != "" {
			opts.FieldSelector += ","
		}
		opts.FieldSelector += "metadata.namespace=" + f.Namespace
	}
	return &opts
}

// informer knows how to turn a cache.SharedIndexInformer into a Collection[T].
type informer[T runtime.Object] struct {
	collectionShared
	inf    cache.SharedIndexInformer
	stop   chan struct{}
	synced chan struct{}
	syncer *channelSyncer
}

// GetKey retrieves an object by its key. For an informer, this must be the namespace and name of the object.
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

func (i *informer[T]) Register(f func(ev Event[T])) Registration {
	reg, err := i.inf.AddEventHandler(eventHandler[T]{
		handler: func(ev Event[T], syncing bool) {
			f(ev)
		},
	})
	if err != nil {
		i.logger().Error("error registering informer handler", "err", err)
	}

	return newInformerRegistration(i, reg)
}

func (i *informer[T]) RegisterBatched(f func(ev []Event[T]), runExistingState bool) Registration {
	reg, err := i.inf.AddEventHandler(eventHandler[T]{
		handler: func(ev Event[T], syncing bool) {
			f([]Event[T]{ev})
		},
	})
	if err != nil {
		i.logger().Error("error registering informer event handler", "err", err)
	}

	return newInformerRegistration(i, reg)
}

func (i *informer[T]) WaitUntilSynced(stop <-chan struct{}) (result bool) {
	return i.syncer.WaitUntilSynced(stop)
}

func (i *informer[T]) HasSynced() bool {
	return i.syncer.HasSynced()
}

func (i *informer[T]) Index(e KeyExtractor[T]) Index[T] {
	idxKey := fmt.Sprintf("%p", e) // map key is based on the extractor func.

	err := i.inf.AddIndexers(map[string]cache.IndexFunc{
		idxKey: func(obj any) ([]string, error) {
			t := extractRuntimeObject[T](obj)
			if t == nil {
				return nil, nil
			}
			return e(*t), nil
		},
	})

	if err != nil {
		i.logger().Error("failed to add requested indexer", "err", err)
	}
	return &informerIndexer[T]{idxKey: idxKey, inf: i.inf.GetIndexer(), extractor: e, parentName: i.name}
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

	idxKey    string
	inf       cache.Indexer
	extractor KeyExtractor[T]
}

func (i *informerIndexer[T]) Lookup(key string) []T {
	res, err := i.inf.ByIndex(i.idxKey, key)
	if err != nil {
		slog.Error("indexer failed to perform key lookup", "err", err, "key", key, "parentName", i.parentName)
	}
	result := make([]T, 0, len(res))
	for _, obj := range res {
		result = append(result, obj.(T))
	}
	return result
}

func (i *informerIndexer[T]) objectHasKey(t T, key string) bool { //nolint: unused // for interface
	for _, got := range i.extractor(t) {
		if got == key {
			return true
		}
	}
	return false
}

type eventHandler[T runtime.Object] struct {
	handler func(ev Event[T], syncing bool)
}

func (e eventHandler[T]) OnAdd(obj any, isInInitialList bool) {
	e.handler(Event[T]{
		New:  extractRuntimeObject[T](obj),
		Type: EventAdd,
	}, isInInitialList)
}

func (e eventHandler[T]) OnUpdate(oldObj, newObj any) {
	e.handler(Event[T]{
		Old:  extractRuntimeObject[T](oldObj),
		New:  extractRuntimeObject[T](newObj),
		Type: EventUpdate,
	}, false)
}

func (e eventHandler[T]) OnDelete(obj any) {
	e.handler(Event[T]{
		Old:  extractRuntimeObject[T](obj),
		Type: EventDelete,
	}, false)
}

func zero[T comparable]() T {
	var z T
	return z
}

func metaOptionsToCtrlOptions(opts metav1.ListOptions) []client.ListOption {
	var result []client.ListOption
	if opts.Limit > 0 {
		result = append(result, client.Limit(opts.Limit))
	}
	if opts.Continue != "" {
		result = append(result, client.Continue(opts.Continue))
	}
	if opts.LabelSelector != "" {
		ls, err := labels.Parse(opts.LabelSelector)
		if err == nil {
			result = append(result, client.MatchingLabelsSelector{Selector: ls})
		}
	}
	if opts.FieldSelector != "" {
		fs, err := fields.ParseSelector(opts.FieldSelector)
		if err == nil {
			result = append(result, client.MatchingFieldsSelector{Selector: fs})
		}
	}
	return result
}

// extractRuntimeObject retrieves a runtime.Object from its argument.
func extractRuntimeObject[T runtime.Object](obj any) *T {
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
// *corev1.PodList.
type TypedClient[TL runtime.Object] interface {
	List(ctx context.Context, opts metav1.ListOptions) (TL, error)
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
}

type informerRegistration[T runtime.Object] struct {
	parent *informer[T]
	reg    cache.ResourceEventHandlerRegistration
	syncer *pollingSyncer
}

func newInformerRegistration[T runtime.Object](parent *informer[T], reg cache.ResourceEventHandlerRegistration) *informerRegistration[T] {
	return &informerRegistration[T]{
		parent: parent,
		reg:    reg,
		syncer: &pollingSyncer{
			interval:  *parent.pollInterval,
			hasSynced: reg.HasSynced,
		},
	}
}

func (r *informerRegistration[T]) Unregister() {
	if err := r.parent.inf.RemoveEventHandler(r.reg); err != nil {
		r.parent.logger().Error("failed to remove eventHandler", "err", err)
	}
}

func (r *informerRegistration[T]) WaitUntilSynced(stop <-chan struct{}) bool {
	return r.syncer.WaitUntilSynced(stop)
}

func (r *informerRegistration[T]) HasSynced() bool {
	return r.reg.HasSynced()
}
