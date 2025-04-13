package pkg_test

import (
	"context"
	krtlite "github.com/kalexmills/krt-plusplus/pkg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"sync"
	"sync/atomic"
	"testing"
)

type SimpleService struct {
	Named
	Selector map[string]string
}

func NewSimpleService(name, namespace string, selector map[string]string) SimpleService {
	return SimpleService{
		Named:    Named{Name: name, Namespace: namespace},
		Selector: selector,
	}
}

func SimpleServiceCollection(services krtlite.Collection[*corev1.Service]) krtlite.Collection[SimpleService] {
	return krtlite.Map(services, func(i *corev1.Service) *SimpleService {
		return &SimpleService{
			Named:    NewNamed(i),
			Selector: i.Spec.Selector,
		}
	})
}

func TestDerivedCollectionSimple(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client := fake.NewClientset()
	nsClient := client.CoreV1().Namespaces()

	// create collections
	Namespaces := krtlite.NewInformer[*corev1.Namespace, *corev1.NamespaceList](ctx, nsClient)
	Namespaces.WaitUntilSynced(ctx.Done())
	SimpleNamespaces := SimpleNamespaceCollection(Namespaces)

	assert.Empty(t, ListSorted(SimpleNamespaces), "expected collection to start empty")

	// add namespace
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns-1"}}
	ns, err := nsClient.Create(ctx, ns, metav1.CreateOptions{})
	assert.NoError(t, err)

	AssertEventually(t, CollectionKeysMatch(SimpleNamespaces, "ns-1"),
		"expected collection to sync")

	// add labels to namespace and assert update is seen
	ns.Labels = map[string]string{
		"foo": "bar",
	}

	_, err = nsClient.Update(ctx, ns, metav1.UpdateOptions{})
	assert.NoError(t, err)

	AssertEventually(t, CollectionContentsDeepEquals(SimpleNamespaces, NewSimpleNamespace("ns-1", map[string]string{"foo": "bar"})),
		"expected updates to propagate when labels added")

	// modify labels and assert we see updates
	ns.Labels["foo"] = "baz-updated"

	_, err = nsClient.Update(ctx, ns, metav1.UpdateOptions{})
	assert.NoError(t, err)

	AssertEventually(t, CollectionContentsDeepEquals(SimpleNamespaces, NewSimpleNamespace("ns-1", map[string]string{"foo": "baz-updated"})),
		"expected updates to propagate when labels modified")

	// add a new handler and assert it gets synced.
	tt := NewTracker[SimpleNamespace](t)
	SimpleNamespaces.Register(tt.Track)
	tt.Wait("add/ns-1")

	// delete and assert we see deletion events
	err = nsClient.Delete(ctx, ns.Name, metav1.DeleteOptions{})
	assert.NoError(t, err)
	tt.Wait("delete/ns-1")
}

func TestDerivedCollectionInitialState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	c := fake.NewClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod",
				Namespace: "namespace",
			},
			Status: corev1.PodStatus{PodIP: "1.2.3.4"},
		},
		&corev1.Pod{ // should be ignored since IP is not set.
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-2",
				Namespace: "namespace",
			},
		},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "svc",
				Namespace: "namespace",
			},
			Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "foo"}},
		},
	)
	pods := krtlite.NewInformer[*corev1.Pod](ctx, c.CoreV1().Pods("namespace"))
	services := krtlite.NewInformer[*corev1.Service](ctx, c.CoreV1().Services("namespace"))

	// assert that collections are equal immediately after waiting for sync.
	SimplePods := SimplePodCollection(pods)
	assert.Equal(t, SimplePods.WaitUntilSynced(ctx.Done()), true)
	CollectionContentsDeepEquals(SimplePods, NewSimplePod("pod", "namespace", "1.2.3.4", nil))

	SimpleServices := SimpleServiceCollection(services)
	assert.Equal(t, SimpleServices.WaitUntilSynced(ctx.Done()), true)
	CollectionContentsDeepEquals(SimpleServices, NewSimpleService("svc", "namespace", map[string]string{"app": "foo"}))

	// TODO: include SimpleEndpoints once Fetch has been implemented
}

func TestCollectionHandlerSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	c := fake.NewClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod",
			Namespace: "namespace",
			Labels:    map[string]string{"app": "foo"},
		},
		Status: corev1.PodStatus{PodIP: "1.2.3.4"},
	})
	Pods := krtlite.NewInformer[*corev1.Pod](ctx, c.CoreV1().Pods("namespace"))

	SimplePods := SimplePodCollection(Pods)

	var (
		gotEvent  atomic.Bool
		reg       cache.ResourceEventHandlerRegistration
		startSync sync.WaitGroup // wait group to satisfy race detector
	)
	startSync.Add(1)
	reg1Delayed := SimplePods.Register(func(o krtlite.Event[SimplePod]) {
		startSync.Wait()
		assert.Equal(t, false, reg.HasSynced())
		gotEvent.Store(true)
	})
	reg = reg1Delayed // satisfy race detector
	startSync.Done()

	ok := cache.WaitForCacheSync(ctx.Done(), reg.HasSynced)
	require.True(t, ok)

	assert.EqualValues(t, true, gotEvent.Load())

}
