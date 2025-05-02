package krt_lite_test

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/cache"
	"sync"
	"sync/atomic"
	"testing"

	krtlite "github.com/kalexmills/krt-lite"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestNewInformer(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	c := fake.NewClientset()
	cmClient := c.CoreV1().ConfigMaps("ns")

	ConfigMaps := krtlite.NewTypedClientInformer[*corev1.ConfigMap](ctx, c.CoreV1().ConfigMaps(metav1.NamespaceAll))

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

	_, err := cmClient.Create(ctx, cmA, metav1.CreateOptions{})
	require.NoError(t, err)
	tt.Wait("add/ns/a")

	cmA2, err = cmClient.Update(ctx, cmA2, metav1.UpdateOptions{})
	require.NoError(t, err)
	tt.Wait("update/ns/a")

	cmB, err = cmClient.Create(ctx, cmB, metav1.CreateOptions{})
	require.NoError(t, err)
	tt.Wait("add/ns/b")

	assert.True(t, CollectionKeysMatch(ConfigMaps, "ns/a", "ns/b")())

	assert.Equal(t, ConfigMaps.GetKey("ns/b"), &cmB)
	assert.Equal(t, ConfigMaps.GetKey("ns/a"), &cmA2)

	tt2 := NewTracker[*corev1.ConfigMap](t)
	ConfigMaps.Register(tt2.Track)
	tt2.Wait("add/ns/a", "add/ns/b")

	err = c.CoreV1().ConfigMaps("ns").Delete(ctx, cmB.Name, metav1.DeleteOptions{})
	require.NoError(t, err)
	tt2.Wait("delete/ns/b")
}

func TestInformerSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	c := fake.NewClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "name",
			Namespace: "namespace",
		},
	})

	ConfigMaps := krtlite.NewTypedClientInformer[*corev1.ConfigMap](ctx, c.CoreV1().ConfigMaps(metav1.NamespaceAll))

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
