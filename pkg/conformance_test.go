package pkg_test

import (
	"context"
	"fmt"
	krtlite "github.com/kalexmills/krt-lite/pkg"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"math/rand"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"strings"
	"testing"
	"time"
)

type Rig[T any] interface {
	krtlite.Collection[T]
	CreateObject(ctx context.Context, key string)
}

type informerRig struct {
	krtlite.Collection[*corev1.ConfigMap]
	client client.WithWatch
}

func (r *informerRig) CreateObject(ctx context.Context, key string) {
	ns, name, _ := strings.Cut(key, "/")

	_ = r.client.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
	})
}

type mergeRig struct {
	krtlite.Collection[Named]
	inner [2]krtlite.StaticCollection[Named]
	idx   int
}

func (r *mergeRig) CreateObject(ctx context.Context, key string) {
	idx := r.idx
	r.idx = (r.idx + 1) % len(r.inner) // Switch which collection we add to each time
	ns, name, _ := strings.Cut(key, "/")
	r.inner[idx].Update(Named{Namespace: ns, Name: name})
}

type derivedRig struct {
	krtlite.Collection[Named]
	names      krtlite.StaticCollection[string]
	namespaces krtlite.StaticCollection[string]
}

func (r *derivedRig) CreateObject(ctx context.Context, key string) {
	ns, name, _ := strings.Cut(key, "/")
	r.namespaces.Update(ns)
	r.names.Update(name)
}

type staticRig struct {
	krtlite.StaticCollection[Named]
}

func (r *staticRig) CreateObject(ctx context.Context, key string) {
	ns, name, _ := strings.Cut(key, "/")
	r.Update(Named{Namespace: ns, Name: name})
}

func TestConformance(t *testing.T) {
	t.Run("informer", func(t *testing.T) {
		ctx := t.Context()

		c := fake.NewFakeClient()
		col := krtlite.NewInformer[*corev1.ConfigMap, corev1.ConfigMapList](ctx, c, krtlite.WithStop(ctx.Done()))
		rig := &informerRig{
			Collection: col,
			client:     c,
		}
		runConformance[*corev1.ConfigMap](t, rig)
	})
	t.Run("staticCollection", func(t *testing.T) {
		ctx := t.Context()

		col := krtlite.NewStaticCollection[Named](nil, nil, krtlite.WithStop(ctx.Done()))
		rig := &staticRig{StaticCollection: col}
		runConformance[Named](t, rig)
	})

	t.Run("mergeDisjoint", func(t *testing.T) {
		ctx := t.Context()
		col1 := krtlite.NewStaticCollection[Named](nil, nil, krtlite.WithStop(ctx.Done()))
		col2 := krtlite.NewStaticCollection[Named](nil, nil, krtlite.WithStop(ctx.Done()))
		j := krtlite.MergeDisjoint[Named]([]krtlite.Collection[Named]{col1, col2},
			krtlite.WithStop(ctx.Done()), krtlite.WithName("MergeDisjoint"))
		rig := &mergeRig{
			Collection: j,
			inner:      [2]krtlite.StaticCollection[Named]{col1, col2},
		}
		runConformance[Named](t, rig)
	})

	t.Run("derivedCollection", func(t *testing.T) {
		ctx := t.Context()
		namespaces := krtlite.NewStaticCollection[string](nil, nil, krtlite.WithStop(ctx.Done()), krtlite.WithName("namespaces"))
		names := krtlite.NewStaticCollection[string](nil, nil, krtlite.WithStop(ctx.Done()), krtlite.WithName("names"))
		col := krtlite.FlatMap(namespaces, func(ctx krtlite.Context, ns string) []Named {
			names := krtlite.Fetch[string](ctx, names)
			var result []Named
			for _, n := range names {
				result = append(result, Named{Namespace: ns, Name: n})
			}
			return result
		}, krtlite.WithStop(ctx.Done()), krtlite.WithName("DerivedCollection"))
		rig := &derivedRig{
			Collection: col,
			namespaces: namespaces,
			names:      names,
		}
		runConformance[Named](t, rig)
	})
}

func runConformance[T any](t *testing.T, col Rig[T]) {
	ctx := t.Context()
	stop := t.Context().Done()

	assert.Equal(t, len(col.List()), 0)

	// Register a handler at the start of the collection
	t.Log("registering early handler")
	earlyHandler := NewTracker[T](t)
	earlyHandlerSynced := col.Register(earlyHandler.Track)

	// Ensure collection and handler are synced
	assert.Equal(t, true, col.WaitUntilSynced(stop))
	assert.Equal(t, true, earlyHandlerSynced.WaitUntilSynced(stop))

	// Create an object
	t.Log("creating a/b")
	col.CreateObject(ctx, "a/b")
	earlyHandler.Wait("add/a/b")
	assert.Equal(t, 1, len(col.List()))
	assert.NotNil(t, col.GetKey("a/b"))

	// Now register one later
	t.Log("registering after creation")
	lateHandler := NewTracker[T](t)
	assert.True(t, col.Register(lateHandler.Track).WaitUntilSynced(stop))
	lateHandler.Wait("add/a/b")

	// Handler that will be removed later
	t.Log("add handler to be removed later")
	removeHandler := NewTracker[T](t)
	removeHandlerSynced := col.Register(removeHandler.Track)
	assert.True(t, removeHandlerSynced.WaitUntilSynced(stop))
	removeHandler.Wait("add/a/b")

	delayedSync := col.Register(func(o krtlite.Event[T]) {
		<-stop
	})
	assert.False(t, delayedSync.HasSynced())

	// add another object, we should get it from both handlers
	t.Log("creating a/c")
	col.CreateObject(ctx, "a/c")
	earlyHandler.Wait("add/a/c")
	lateHandler.Wait("add/a/c")
	removeHandler.Wait("add/a/c")
	assert.Len(t, col.List(), 2)
	assert.NotNil(t, col.GetKey("a/b"))
	assert.NotNil(t, col.GetKey("a/c"))

	removeHandlerSynced.Unregister()

	// Add another object.
	t.Log("creating a/d")
	col.CreateObject(ctx, "a/d")
	earlyHandler.Wait("add/a/d")
	lateHandler.Wait("add/a/d")
	assert.Len(t, col.List(), 3)

	var keys []string
	for n := range 20 {
		keys = append(keys, fmt.Sprintf("a/%v", n))
	}

	t.Log("creating many objects")
	go func() {
		for _, k := range keys {
			col.CreateObject(ctx, k)
		}
	}()

	// Introduce jitter to ensure we don't register first
	// nolint: gosec // just for testing
	time.Sleep(time.Microsecond * time.Duration(rand.Int31n(100)))

	t.Log("registering race handler")
	raceHandler := NewTracker[T](t)
	raceHandlerSynced := col.Register(raceHandler.Track)
	assert.Equal(t, true, raceHandlerSynced.WaitUntilSynced(stop))

	want := []string{"add/a/b", "add/a/c", "add/a/d"}
	for _, k := range keys {
		want = append(want, fmt.Sprintf("add/%v", k))
	}
	// We should get every event exactly one time.
	t.Log("waiting for all events")
	raceHandler.Wait(want...)
	raceHandler.Empty()
	removeHandler.Empty()
}
