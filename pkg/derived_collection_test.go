package pkg_test

import (
	"context"
	krtlite "github.com/kalexmills/krt-plusplus/pkg"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
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
	return krtlite.Map(services, func(ctx krtlite.Context, i *corev1.Service) *SimpleService {
		return &SimpleService{
			Named:    NewNamed(i),
			Selector: i.Spec.Selector,
		}
	}, krtlite.WithName("SimpleService"))
}

func TestDerivedCollectionSimple(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout*5)
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
	ctx, cancel := context.WithTimeout(context.Background(), timeout*5)
	defer cancel()

	c := fake.NewClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod",
				Namespace: "namespace",
				Labels:    map[string]string{"app": "foo"},
			},
			Status: corev1.PodStatus{PodIP: "1.2.3.4"},
		},
		&corev1.Pod{ // should be ignored since IP is not set.
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-2",
				Namespace: "namespace",
			},
			Status: corev1.PodStatus{PodIP: "0.0.0.0"},
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
	SimpleServices := SimpleServiceCollection(services)
	SimpleEndpoints := SimpleEndpointsCollection(SimplePods, SimpleServices)
	assert.Equal(t, true, SimpleEndpoints.WaitUntilSynced(ctx.Done()))

	assert.Equal(t, []SimpleEndpoint{{Pod: "pod", Service: "svc", Namespace: "namespace", IP: "1.2.3.4"}},
		ListSorted(SimpleEndpoints))
}

func TestCollectionMerged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout*5)
	defer cancel()

	c := fake.NewClientset()
	podClient := c.CoreV1().Pods("namespace")
	pods := krtlite.NewInformer[*corev1.Pod](ctx, podClient, krtlite.WithName("Pods"))
	svcClient := c.CoreV1().Services("namespace")
	services := krtlite.NewInformer[*corev1.Service](ctx, svcClient, krtlite.WithName("Services"))

	// TODO: not waiting for informers to start causes messages to be dropped.
	pods.WaitUntilSynced(ctx.Done())
	services.WaitUntilSynced(ctx.Done())

	SimplePods := SimplePodCollection(pods)
	SimpleServices := SimpleServiceCollection(services)
	SimpleEndpoints := SimpleEndpointsCollection(SimplePods, SimpleServices)

	assert.Empty(t, ListSorted(SimpleEndpoints))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod",
			Namespace: "namespace",
			Labels:    map[string]string{"app": "foo"},
		},
	}
	pod, err := podClient.Create(ctx, pod, metav1.CreateOptions{})

	assert.NoError(t, err)
	assert.Empty(t, ListSorted(SimpleEndpoints), "expected no endpoints")

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc",
			Namespace: "namespace",
		},
		Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "foo"}},
	}
	svc, err = svcClient.Create(ctx, svc, metav1.CreateOptions{})

	assert.NoError(t, err)
	assert.Empty(t, ListSorted(SimpleEndpoints), "expected no endpoints")

	pod.Status = corev1.PodStatus{PodIP: "1.2.3.4"}
	pod, err = podClient.Update(ctx, pod, metav1.UpdateOptions{})
	assert.NoError(t, err)

	AssertEventually(t, CollectionContentsDeepEquals(SimpleEndpoints,
		SimpleEndpoint{Pod: "pod", Service: "svc", Namespace: "namespace", IP: "1.2.3.4"},
	), "expected an endpoint once pod is assigned an IP")

	pod.Status.PodIP = "1.2.3.5"
	pod, err = podClient.UpdateStatus(ctx, pod, metav1.UpdateOptions{})
	assert.NoError(t, err)

	AssertEventually(t, CollectionContentsDeepEquals(SimpleEndpoints,
		SimpleEndpoint{Pod: "pod", Service: "svc", Namespace: "namespace", IP: "1.2.3.5"},
	), "expected endpoints to change when podIP changes")

	err = podClient.Delete(ctx, pod.Name, metav1.DeleteOptions{})
	assert.NoError(t, err)

	AssertEventually(t, CollectionContentsDeepEquals(SimpleEndpoints),
		"expected endpoints to be deleted when pod is removed") // empty

	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "name2",
			Namespace: "namespace",
			Labels:    map[string]string{"app": "foo"},
		},
		Status: corev1.PodStatus{PodIP: "2.3.4.5"},
	}
	_, err = podClient.Create(ctx, pod, metav1.CreateOptions{})
	assert.NoError(t, err)
	_, err = podClient.Create(ctx, pod2, metav1.CreateOptions{})
	assert.NoError(t, err)

	AssertEventually(t, CollectionContentsDeepEquals(SimpleEndpoints,
		SimpleEndpoint{pod2.Name, svc.Name, pod2.Namespace, pod2.Status.PodIP},
		SimpleEndpoint{pod.Name, svc.Name, pod.Namespace, pod.Status.PodIP},
	), "expected multiple endpoints once pods are created")
}
