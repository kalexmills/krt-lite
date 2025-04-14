package pkg_test

import (
	"context"
	"maps"
	"slices"
	"testing"

	krtlite "github.com/kalexmills/krt-plusplus/pkg"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	clientcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

func TestIndex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	tests := []struct {
		name      string
		makeIndex func(c clientcorev1.ConfigMapInterface) krtlite.IndexableCollection[*corev1.ConfigMap]
	}{
		{
			name: "Informer",
			makeIndex: func(c clientcorev1.ConfigMapInterface) krtlite.IndexableCollection[*corev1.ConfigMap] {
				return krtlite.NewInformer[*corev1.ConfigMap](ctx, c)
			},
		},
		{
			name: "Map",
			makeIndex: func(c clientcorev1.ConfigMapInterface) krtlite.IndexableCollection[*corev1.ConfigMap] {
				inf := krtlite.NewInformer[*corev1.ConfigMap](ctx, c)
				return krtlite.Map[*corev1.ConfigMap, *corev1.ConfigMap](inf, func(ctx krtlite.Context, cm *corev1.ConfigMap) **corev1.ConfigMap {
					return &cm
				})
			},
		},
		{
			name: "FlatMap", // include both Map + FlatMap in case implementations later change
			makeIndex: func(c clientcorev1.ConfigMapInterface) krtlite.IndexableCollection[*corev1.ConfigMap] {
				inf := krtlite.NewInformer[*corev1.ConfigMap](ctx, c)
				return krtlite.FlatMap[*corev1.ConfigMap, *corev1.ConfigMap](inf, func(ctx krtlite.Context, cm *corev1.ConfigMap) []*corev1.ConfigMap {
					return []*corev1.ConfigMap{cm}
				})
			},
		},
		// TODO: test Joiner once we have filtered collections.
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			c := fake.NewClientset()
			cmClient := c.CoreV1().ConfigMaps("ns")

			cmA := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "a",
					Namespace: "ns",
				},
				Data: map[string]string{"shared-ab": "data-a"},
			}
			cmB := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "b",
					Namespace: "ns",
				},
				Data: map[string]string{
					"shared-ab": "data-b",
				},
			}
			cmC := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "collection",
					Namespace: "ns",
				},
				Data: map[string]string{"shared-bc": "data-collection"},
			}

			ConfigMaps := tt.makeIndex(c.CoreV1().ConfigMaps("ns"))

			tt := NewTracker[*corev1.ConfigMap](t)
			ConfigMaps.Register(tt.Track)

			assert.Empty(t, ConfigMaps.List())

			idx := ConfigMaps.Index(func(t *corev1.ConfigMap) []string {
				return slices.Collect(maps.Keys(t.Data))
			})

			assert.Empty(t, idx.Lookup("shared-ab"))
			assert.Empty(t, idx.Lookup("shared-bc"))

			lookup := func(key string) func() any {
				return func() any {
					return idx.Lookup(key)
				}
			}

			cmA, err := cmClient.Create(ctx, cmA, metav1.CreateOptions{})
			assert.NoError(t, err)

			AssertEventuallyEqual(t, []*corev1.ConfigMap{cmA}, lookup("shared-ab"))

			cmC, err = cmClient.Create(ctx, cmC, metav1.CreateOptions{})
			assert.NoError(t, err)

			AssertEventuallyEqual(t, []*corev1.ConfigMap{cmA}, lookup("shared-ab"))
			AssertEventuallyEqual(t, []*corev1.ConfigMap{cmC}, lookup("shared-bc"))

			cmB, err = cmClient.Create(ctx, cmB, metav1.CreateOptions{})
			assert.NoError(t, err)

			AssertEventuallyEqual(t, []*corev1.ConfigMap{cmA, cmB}, lookup("shared-ab"))
			AssertEventuallyEqual(t, []*corev1.ConfigMap{cmC}, lookup("shared-bc"))

			cmB.Data["shared-bc"] = "data"
			cmB, err = cmClient.Update(ctx, cmB, metav1.UpdateOptions{})
			assert.NoError(t, err)

			AssertEventuallyEqual(t, []*corev1.ConfigMap{cmA, cmB}, lookup("shared-ab"))
			AssertEventuallyEqual(t, []*corev1.ConfigMap{cmB, cmC}, lookup("shared-bc"))

			err = cmClient.Delete(ctx, cmB.Name, metav1.DeleteOptions{})
			assert.NoError(t, err)

			AssertEventuallyEqual(t, []*corev1.ConfigMap{cmA}, lookup("shared-ab"))
			AssertEventuallyEqual(t, []*corev1.ConfigMap{cmC}, lookup("shared-bc"))
		})
	}
}
