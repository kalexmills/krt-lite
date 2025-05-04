package krtlite_test

import (
	"context"
	krtlite "github.com/kalexmills/krt-lite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	typedfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sync"
	"sync/atomic"
	"testing"
)

type rig interface {
	Collection(ctx context.Context, opts ...krtlite.CollectionOption) krtlite.Collection[*corev1.ConfigMap]
	Create(ctx context.Context, t *corev1.ConfigMap) (*corev1.ConfigMap, error)
	Update(ctx context.Context, t *corev1.ConfigMap) (*corev1.ConfigMap, error)
	Delete(ctx context.Context, t *corev1.ConfigMap) error
}

type typedClientRig struct {
	c *typedfake.Clientset
}

func (r typedClientRig) Collection(ctx context.Context, opts ...krtlite.CollectionOption) krtlite.Collection[*corev1.ConfigMap] {
	return krtlite.NewTypedClientInformer[*corev1.ConfigMap](ctx, r.c.CoreV1().ConfigMaps(""), opts...)
}

func (r typedClientRig) Create(ctx context.Context, t *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	return r.c.CoreV1().ConfigMaps(t.Namespace).Create(ctx, t, metav1.CreateOptions{}) //nolint: wrapcheck
}

func (r typedClientRig) Update(ctx context.Context, t *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	return r.c.CoreV1().ConfigMaps(t.Namespace).Update(ctx, t, metav1.UpdateOptions{}) //nolint: wrapcheck
}

func (r typedClientRig) Delete(ctx context.Context, t *corev1.ConfigMap) error {
	return r.c.CoreV1().ConfigMaps(t.Namespace).Delete(ctx, t.Name, metav1.DeleteOptions{}) //nolint: wrapcheck
}

type clientRig struct {
	c client.WithWatch
}

func (r clientRig) Collection(ctx context.Context, opts ...krtlite.CollectionOption) krtlite.Collection[*corev1.ConfigMap] {
	return krtlite.NewInformer[*corev1.ConfigMap, corev1.ConfigMapList](ctx, r.c, opts...)
}

func (r clientRig) Create(ctx context.Context, t *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	err := r.c.Create(ctx, t)
	return t, err //nolint: wrapcheck
}

func (r clientRig) Update(ctx context.Context, t *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	err := r.c.Update(ctx, t)
	return t, err //nolint: wrapcheck
}

func (r clientRig) Delete(ctx context.Context, t *corev1.ConfigMap) error {
	err := r.c.Delete(ctx, t)
	return err //nolint: wrapcheck
}

func TestInformer(t *testing.T) {
	doTest := func(t *testing.T, r rig) {
		ctx, cancel := context.WithTimeout(t.Context(), timeout)
		defer cancel()

		ConfigMaps := r.Collection(ctx, krtlite.WithContext(ctx))

		tt := NewTracker[*corev1.ConfigMap](t)
		ConfigMaps.Register(tt.Track)

		assert.Empty(t, ConfigMaps.List())

		cmA := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "a",
				Namespace: "ns",
			},
		}
		cmA2 := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "a",
				Namespace: "ns",
			},
			Data: map[string]string{"foo": "bar"},
		}
		cmB := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "b",
				Namespace: "ns",
			},
		}

		_, err := r.Create(ctx, cmA)
		require.NoError(t, err)
		tt.Wait("add/ns/a")

		cmA2, err = r.Update(ctx, cmA2)
		require.NoError(t, err)
		tt.Wait("update/ns/a")

		cmB, err = r.Create(ctx, cmB)
		require.NoError(t, err)
		tt.Wait("add/ns/b")

		assert.True(t, CollectionKeysMatch(ConfigMaps, "ns/a", "ns/b")())

		assert.Equal(t, ConfigMaps.GetKey("ns/b"), &cmB)
		assert.Equal(t, ConfigMaps.GetKey("ns/a"), &cmA2)

		tt2 := NewTracker[*corev1.ConfigMap](t)
		ConfigMaps.Register(tt2.Track)
		tt2.Wait("add/ns/a", "add/ns/b")

		err = r.Delete(ctx, cmB)
		require.NoError(t, err)
		tt2.Wait("delete/ns/b")
	}

	t.Run("NewTypedClientInformer", func(t *testing.T) {
		doTest(t, &typedClientRig{
			c: typedfake.NewClientset(),
		})
	})

	t.Run("NewInformer", func(t *testing.T) {
		doTest(t, &clientRig{
			c: fake.NewFakeClient(),
		})
	})
}

func TestInformerFilters(t *testing.T) {
	doTest := func(t *testing.T, r rig) {
		ctx, cancel := context.WithTimeout(t.Context(), timeout)
		defer cancel()

		cmA := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "a",
				Namespace: "ns1",
				Labels: map[string]string{
					"foo": "baz",
				},
			},
		}
		cmB := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "b",
				Namespace: "ns1",
				Labels: map[string]string{
					"foo": "bar",
				},
			},
		}
		cmC := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "c",
				Namespace: "ns2",
				Labels: map[string]string{
					"foo": "baz",
				},
			},
		}

		for _, cm := range []*corev1.ConfigMap{cmA, cmB, cmC} {
			_, err := r.Create(ctx, cm)
			require.NoError(t, err)
		}

		ConfigMaps := r.Collection(ctx, krtlite.WithFilterByNamespace("ns1"))
		tt := NewTracker[*corev1.ConfigMap](t)
		reg := ConfigMaps.Register(tt.Track)

		tt.Wait("add/ns1/a", "add/ns1/b")
		tt.Empty()

		reg.Unregister()

		ConfigMaps = r.Collection(ctx, krtlite.WithFilterByField("metadata.name=c"))
		tt = NewTracker[*corev1.ConfigMap](t)
		reg = ConfigMaps.Register(tt.Track)

		tt.Wait("add/ns2/c")
		tt.Empty()

		reg.Unregister()

		ConfigMaps = r.Collection(ctx, krtlite.WithFilterByLabel("foo=baz"))
		tt = NewTracker[*corev1.ConfigMap](t)
		_ = ConfigMaps.Register(tt.Track)

		tt.Wait("add/ns1/a", "add/ns2/c")
		tt.Empty()
	}

	// TODO: needs an envtest to test NewTypedClientInformer -- k8s fake package doesn't support filtering

	t.Run("NewInformer", func(t *testing.T) {
		doTest(t, &clientRig{
			// fake controller runtime clients require indices for indexing by field. Namespace filters are implemented as
			// field filters.
			c: fake.NewClientBuilder().
				WithIndex(&corev1.ConfigMap{}, "metadata.name",
					func(object client.Object) []string {
						return []string{object.(*corev1.ConfigMap).Name}
					},
				).
				WithIndex(&corev1.ConfigMap{}, "metadata.namespace",
					func(object client.Object) []string {
						return []string{object.(*corev1.ConfigMap).Namespace}
					},
				).
				Build(),
		})
	})
}

func TestTypedClientInformerSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	c := typedfake.NewClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "name",
			Namespace: "namespace",
		},
	})

	ConfigMaps := krtlite.NewTypedClientInformer[*corev1.ConfigMap](ctx, c.CoreV1().ConfigMaps(metav1.NamespaceAll),
		krtlite.WithContext(ctx))

	var (
		gotEvent  atomic.Bool
		reg       cache.ResourceEventHandlerRegistration
		startSync sync.WaitGroup // wait group to satisfy race detector
	)
	startSync.Add(1)
	reg1Delayed := ConfigMaps.Register(func(o krtlite.Event[*corev1.ConfigMap]) {
		startSync.Wait()
		assert.False(t, reg.HasSynced())
		gotEvent.Store(true)
	})
	reg = reg1Delayed // satisfy race detector
	startSync.Done()

	ok := cache.WaitForCacheSync(ctx.Done(), reg.HasSynced)
	require.True(t, ok)

	assert.True(t, gotEvent.Load())
}
