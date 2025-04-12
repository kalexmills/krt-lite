package pkg_test

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"

	krtlite "github.com/kalexmills/krt-plusplus/pkg"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestNewInformer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := fake.NewClientset()
	cmClient := c.CoreV1().ConfigMaps("ns")

	ConfigMaps := krtlite.NewInformer[*corev1.ConfigMap](ctx, c.CoreV1().ConfigMaps(metav1.NamespaceAll))

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

	cmA, err := cmClient.Create(ctx, cmA, metav1.CreateOptions{})
	assert.NoError(t, err)
	tt.Wait("add/ns/a")

	cmA2, err = cmClient.Update(ctx, cmA2, metav1.UpdateOptions{})
	assert.NoError(t, err)
	tt.Wait("update/ns/a")

	cmB, err = cmClient.Create(ctx, cmB, metav1.CreateOptions{})
	assert.NoError(t, err)
	tt.Wait("add/ns/b")

	assert.True(t, CollectionKeysMatch(ConfigMaps, "ns/a", "ns/b")())

	assert.Equal(t, ConfigMaps.GetKey("ns/b"), &cmB)
	assert.Equal(t, ConfigMaps.GetKey("ns/a"), &cmA2)

	tt2 := NewTracker[*corev1.ConfigMap](t)
	ConfigMaps.Register(tt2.Track)
	tt.Wait("add/ns/a", "add/ns/b")

	err = c.CoreV1().ConfigMaps("ns").Delete(ctx, cmB.Name, metav1.DeleteOptions{})
	assert.NoError(t, err)
	tt.Wait("delete/ns/b")
}
