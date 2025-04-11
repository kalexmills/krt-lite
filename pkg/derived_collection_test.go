package pkg

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

const (
	timeout      = time.Second * 2
	pollInterval = 50 * time.Millisecond
)

type SimpleNamespace struct {
	Name   string
	Labels map[string]string
}

func (n SimpleNamespace) ResourceName() string {
	return n.Name
}

func SimpleNamespaceCollection(pods Collection[*corev1.Namespace]) Collection[SimpleNamespace] {
	return Map(pods, func(i *corev1.Namespace) *SimpleNamespace {
		return &SimpleNamespace{
			Name:   i.Name,
			Labels: i.Labels,
		}
	})
}

func TestDerivedCollectionSimple(t *testing.T) {
	t0 := time.Now()

	client := fake.NewClientset()
	nsClient := client.CoreV1().Namespaces()
	ctx := context.Background()

	// create collections
	Namespaces := WrapClient[*corev1.Namespace, *corev1.NamespaceList](ctx, nsClient)
	Namespaces.WaitUntilSynced(t.Context().Done())
	SimpleNamespaces := SimpleNamespaceCollection(Namespaces)

	assert.Empty(t, sorted(SimpleNamespaces), "expected collection to start empty")

	// add namespace
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns-1"}}
	ns, err := nsClient.Create(ctx, ns, metav1.CreateOptions{})
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		return reflect.DeepEqual([]SimpleNamespace{{Name: "ns-1"}}, sorted(SimpleNamespaces))
	}, timeout, pollInterval, "expected collection to sync")

	// add labels to namespace and assert update is seen
	ns.Labels = map[string]string{
		"foo": "bar",
	}

	_, err = nsClient.Update(ctx, ns, metav1.UpdateOptions{})
	assert.NoError(t, err)
	assert.Eventually(t, func() bool {
		return reflect.DeepEqual(sorted(SimpleNamespaces),
			[]SimpleNamespace{{Name: "ns-1", Labels: map[string]string{"foo": "bar"}}},
		)
	}, timeout, pollInterval, "expected updates to propagate")

	// modify labels and assert we see updates
	ns.Labels["foo"] = "baz-updated"

	_, err = nsClient.Update(ctx, ns, metav1.UpdateOptions{})
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		return reflect.DeepEqual(sorted(SimpleNamespaces),
			[]SimpleNamespace{{Name: "ns-1", Labels: map[string]string{"foo": "baz-updated"}}},
		)
	}, timeout, pollInterval, "expected updates to propagate")

	// add a new handler and assert it gets synced.
	tt := NewTracker[SimpleNamespace](t)
	SimpleNamespaces.Register(tt.Track)
	tt.Wait("add/ns-1")

	// delete and assert we see deletion events
	err = nsClient.Delete(ctx, ns.Name, metav1.DeleteOptions{})
	assert.NoError(t, err)
	tt.Wait("delete/ns-1")

}

func sorted[T ResourceNamer](c Collection[T]) []T {
	result := c.List()
	slices.SortFunc(result, func(a, b T) int {
		return strings.Compare(a.ResourceName(), b.ResourceName())
	})
	return result
}

type tracker[T ResourceNamer] struct {
	t      *testing.T
	events map[string]struct{}
}

func NewTracker[T ResourceNamer](t *testing.T) tracker[T] {
	return tracker[T]{
		t:      t,
		events: make(map[string]struct{}),
	}
}

func (t *tracker[T]) Track(e Event[T]) {
	t.events[fmt.Sprintf("%s/%s", e.Event, e.Latest().ResourceName())] = struct{}{}
}

func (t *tracker[T]) Wait(events ...string) {
	assert.Eventually(t.t, func() bool {
		for _, ev := range events {
			if _, ok := t.events[ev]; !ok {
				return false
			}
		}
		return true
	}, timeout, pollInterval)
}
