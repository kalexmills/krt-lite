package pkg_test

import (
	"context"
	"fmt"
	krtlite "github.com/kalexmills/krt-plusplus/pkg"
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

type joinRig struct {
	krtlite.Collection[Named]
	inner [2]krtlite.Singleton[Named]
	idx   int
}

func (r *joinRig) CreateObject(ctx context.Context, key string) {
	idx := r.idx
	r.idx = (r.idx + 1) % len(r.inner) // Switch which collection we add to each time
	ns, name, _ := strings.Cut(key, "/")
	r.inner[idx].Set(&Named{Namespace: ns, Name: name})
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
	t.Run("join", func(t *testing.T) {
		t.Skip("TODO: StaticCollection") // TODO: static collection
		takeFirst := func(ns []Named) Named { return ns[0] }
		ctx := t.Context()
		col1 := krtlite.NewSingleton[Named](nil, true, krtlite.WithStop(ctx.Done()))
		col2 := krtlite.NewSingleton[Named](nil, true, krtlite.WithStop(ctx.Done()))
		j := krtlite.Join[Named]([]krtlite.Collection[Named]{col1, col2}, takeFirst, krtlite.WithStop(ctx.Done()))
		rig := &joinRig{
			Collection: j,
			inner:      [2]krtlite.Singleton[Named]{col1, col2},
		}
		runConformance[Named](t, rig)
	})
	// TODO: joinDisjoint
	t.Run("derivedCollection", func(t *testing.T) {
		// TODO: more of these.
	})
}

func runConformance[T any](t *testing.T, col Rig[T]) {
	ctx := t.Context()
	stop := t.Context().Done()

	assert.Equal(t, len(col.List()), 0)

	// Register a handler at the start of the collection
	earlyHandler := NewTracker[T](t)
	earlyHandlerSynced := col.Register(earlyHandler.Track)

	// Ensure collection and handler are synced
	assert.Equal(t, true, col.WaitUntilSynced(stop))
	assert.Equal(t, true, earlyHandlerSynced.WaitUntilSynced(stop))

	// Create an object
	col.CreateObject(ctx, "a/b")
	earlyHandler.Wait("add/a/b")
	assert.Equal(t, 1, len(col.List()))
	assert.NotNil(t, col.GetKey("a/b"))

	// Now register one later
	lateHandler := NewTracker[T](t)
	assert.True(t, col.Register(lateHandler.Track).WaitUntilSynced(stop))
	lateHandler.Wait("add/a/b")

	// Handler that will be removed later
	removeHandler := NewTracker[T](t)
	removeHandlerSynced := col.Register(removeHandler.Track)
	assert.True(t, removeHandlerSynced.WaitUntilSynced(stop))
	removeHandler.Wait("add/a/b")

	delayedSync := col.Register(func(o krtlite.Event[T]) {
		<-stop
	})
	assert.False(t, delayedSync.HasSynced())

	// add another object, we should get it from both handlers
	col.CreateObject(ctx, "a/c")
	earlyHandler.Wait("add/a/c")
	lateHandler.Wait("add/a/c")
	removeHandler.Wait("add/a/c")
	assert.Len(t, col.List(), 2)
	assert.NotNil(t, col.GetKey("a/b"))
	assert.NotNil(t, col.GetKey("a/c"))

	// TODO: implement UnregisterHandler then enable this
	// removeHandlerSynced.UnregisterHandler()

	// Add another object.
	col.CreateObject(ctx, "a/d")
	earlyHandler.Wait("add/a/d")
	lateHandler.Wait("add/a/d")
	assert.Len(t, col.List(), 3)

	var keys []string
	for n := range 20 {
		keys = append(keys, fmt.Sprintf("a/%v", n))
	}

	raceHandler := NewTracker[T](t)
	go func() {
		for _, k := range keys {
			col.CreateObject(ctx, k)
		}
	}()

	// Introduce jitter to ensure we don't register first
	// nolint: gosec // just for testing
	time.Sleep(time.Microsecond * time.Duration(rand.Int31n(100)))

	raceHandlerSynced := col.Register(raceHandler.Track)
	assert.Equal(t, true, raceHandlerSynced.WaitUntilSynced(stop))

	want := []string{"add/a/b", "add/a/c", "add/a/d"}
	for _, k := range keys {
		want = append(want, fmt.Sprintf("add/%v", k))
	}
	// We should get every event exactly one time.
	raceHandler.Wait(want...)
	raceHandler.Empty()
	// removeHandler.Empty() TODO: implement UnregisterHandler then uncomment
}
