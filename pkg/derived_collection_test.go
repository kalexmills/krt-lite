package pkg

import (
	"context"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"slices"
	"strings"
	"testing"
	"time"
)

type SimpleNamespace struct {
	Name string
}

func (n SimpleNamespace) ResourceName() string {
	return n.Name
}

func SimpleNamespaceCollection(pods Collection[*corev1.Namespace]) Collection[SimpleNamespace] {
	return Map(pods, func(i *corev1.Namespace) *SimpleNamespace {
		return &SimpleNamespace{i.Name}
	})
}

func TestDerivedCollectionSimple(t *testing.T) {
	client := fake.NewClientset()
	ctx := context.Background()

	Namespaces := WrapClient[*corev1.Namespace, *corev1.NamespaceList](ctx, client.CoreV1().Namespaces())
	Namespaces.WaitUntilSynced(t.Context().Done())

	SimpleNamespaces := SimpleNamespaceCollection(Namespaces)

	assert.Empty(t, sorted(SimpleNamespaces), "collection should start empty")

	// add namespace
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns-1"}}
	_, err := client.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		return assert.Equal(t, []SimpleNamespace{{Name: "ns-1"}}, sorted(SimpleNamespaces))
	}, 5*time.Second, time.Second)
}

func sorted[T ResourceNamer](c Collection[T]) []T {
	result := c.List()
	slices.SortFunc(result, func(a, b T) int {
		return strings.Compare(a.ResourceName(), b.ResourceName())
	})
	return result
}
