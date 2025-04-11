package pkg_test

import (
	"context"
	"fmt"
	krtlite "github.com/kalexmills/krt-plusplus/pkg"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	timeout      = time.Second * 2
	pollInterval = 50 * time.Millisecond
)

type SimplePod struct {
	Named
	Labeled
	IP string
}

func NewSimplePod(name, namespace, ip string, labels map[string]string) SimplePod {
	return SimplePod{
		Named:   Named{Name: name, Namespace: namespace},
		Labeled: NewLabeled(labels),
		IP:      ip,
	}
}

func SimplePodCollection(pods krtlite.Collection[*corev1.Pod]) krtlite.Collection[SimplePod] {
	return krtlite.Map(pods, func(i *corev1.Pod) *SimplePod {
		if i.Status.PodIP == "" {
			return nil
		}
		return &SimplePod{
			Named:   NewNamed(i),
			Labeled: NewLabeled(i.Labels),
			IP:      i.Status.PodIP,
		}
	})
}

type SimpleNamespace struct {
	Named
	Labeled
}

func NewSimpleNamespace(name string, labels map[string]string) SimpleNamespace {
	return SimpleNamespace{
		Named:   Named{Name: name},
		Labeled: NewLabeled(labels),
	}
}

func (n SimpleNamespace) ResourceName() string {
	return n.Name
}

func SimpleNamespaceCollection(pods krtlite.Collection[*corev1.Namespace]) krtlite.Collection[SimpleNamespace] {
	return krtlite.Map(pods, func(i *corev1.Namespace) *SimpleNamespace {
		return &SimpleNamespace{
			Named:   NewNamed(i),
			Labeled: NewLabeled(i.Labels),
		}
	})
}

type SimpleEndpoint struct {
	Pod       string
	Service   string
	Namespace string
	IP        string
}

func (s SimpleEndpoint) ResourceName() string {
	return s.Namespace + "/" + s.Service + "/" + s.Pod
}

func SimpleEndpointsCollection(pods krtlite.Collection[SimplePod], services krtlite.Collection[SimpleService]) krtlite.Collection[SimpleEndpoint] {
	return nil // TODO: implement once we have an equivalent to Fetch.
}

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := fake.NewClientset()
	nsClient := client.CoreV1().Namespaces()

	// create collections
	Namespaces := krtlite.NewInformer[*corev1.Namespace, *corev1.NamespaceList](ctx, nsClient)
	Namespaces.WaitUntilSynced(ctx.Done())
	SimpleNamespaces := SimpleNamespaceCollection(Namespaces)

	assert.Empty(t, sorted(SimpleNamespaces), "expected collection to start empty")

	// add namespace
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns-1"}}
	ns, err := nsClient.Create(ctx, ns, metav1.CreateOptions{})
	assert.NoError(t, err)

	assert.Eventually(t,
		AssertCollectionNameOrder(SimpleNamespaces, "ns-1"),
		timeout, pollInterval, "expected collection to sync",
	)

	// add labels to namespace and assert update is seen
	ns.Labels = map[string]string{
		"foo": "bar",
	}

	_, err = nsClient.Update(ctx, ns, metav1.UpdateOptions{})
	assert.NoError(t, err)
	assert.Eventually(t,
		AssertCollectionDeepEquals(SimpleNamespaces, NewSimpleNamespace("ns-1", map[string]string{"foo": "bar"})),
		timeout, pollInterval, "expected updates to propagate when labels added",
	)

	// modify labels and assert we see updates
	ns.Labels["foo"] = "baz-updated"

	_, err = nsClient.Update(ctx, ns, metav1.UpdateOptions{})
	assert.NoError(t, err)

	assert.Eventually(t,
		AssertCollectionDeepEquals(SimpleNamespaces, NewSimpleNamespace("ns-1", map[string]string{"foo": "baz-updated"})),
		timeout, pollInterval, "expected updates to propagate when labels modified",
	)

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
	ctx, cancel := context.WithCancel(context.Background())
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
	AssertCollectionDeepEquals(SimplePods, NewSimplePod("pod", "namespace", "1.2.3.4", nil))

	SimpleServices := SimpleServiceCollection(services)
	assert.Equal(t, SimpleServices.WaitUntilSynced(ctx.Done()), true)
	AssertCollectionDeepEquals(SimpleServices, NewSimpleService("svc", "namespace", map[string]string{"app": "foo"}))

	// TODO: include SimpleEndpoints once Fetch has been implemented
}

func TestCollectionHandlerSync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
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
	)
	pods := krtlite.NewInformer[*corev1.Pod](ctx, c.CoreV1().Pods("namespace"))

	var SimplePods krtlite.Collection[SimplePod]
	var reg1, reg2 cache.ResourceEventHandlerRegistration

	tests := []struct {
		name     string
		waitFunc func()
	}{
		{name: "calls register handlers before WaitUntilSynced is done",
			waitFunc: func() {
				SimplePods.WaitUntilSynced(ctx.Done())
			},
		},
		{name: "calls register handlers before all registrations have synced",
			waitFunc: func() {
				cache.WaitForCacheSync(ctx.Done(), reg1.HasSynced, reg2.HasSynced)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pods = krtlite.NewInformer[*corev1.Pod](ctx, c.CoreV1().Pods("namespace"))
			SimplePods = SimplePodCollection(pods)

			gotEvent := atomic.Uint32{}

			var wg sync.WaitGroup // wait group to satisfy race detector
			wg.Add(1)
			reg1Delayed := SimplePods.Register(func(o krtlite.Event[SimplePod]) {
				wg.Wait()
				assert.Equal(t, reg1.HasSynced(), false)
				gotEvent.Add(1)
			})
			reg2Delayed := SimplePods.Register(func(o krtlite.Event[SimplePod]) {
				wg.Wait()
				assert.Equal(t, reg2.HasSynced(), false)
				gotEvent.Add(1)
			})
			reg1, reg2 = reg1Delayed, reg2Delayed // satisfy race detector
			wg.Done()

			tt.waitFunc()
			assert.EqualValues(t, gotEvent.Load(), 2)
		})
	}
}

func sorted[T krtlite.ResourceNamer](c krtlite.Collection[T]) []T {
	result := c.List()
	slices.SortFunc(result, func(a, b T) int {
		return strings.Compare(a.ResourceName(), b.ResourceName())
	})
	return result
}

type tracker[T krtlite.ResourceNamer] struct {
	t *testing.T

	mut    *sync.RWMutex
	events map[string]struct{}
}

func NewTracker[T krtlite.ResourceNamer](t *testing.T) tracker[T] {
	return tracker[T]{
		t:      t,
		mut:    &sync.RWMutex{},
		events: make(map[string]struct{}),
	}
}

func (t *tracker[T]) Track(e krtlite.Event[T]) {
	t.mut.Lock()
	defer t.mut.Unlock()
	t.events[fmt.Sprintf("%s/%s", e.Event, e.Latest().ResourceName())] = struct{}{}
}

func (t *tracker[T]) Wait(events ...string) {
	assert.Eventually(t.t, func() bool {
		t.mut.RLock()
		defer t.mut.RUnlock()
		for _, ev := range events {
			if _, ok := t.events[ev]; !ok {
				return false
			}
		}
		return true
	}, timeout, pollInterval)
}

type Named struct {
	Namespace string
	Name      string
}

func (s Named) ResourceName() string {
	return s.Namespace + "/" + s.Name
}
func (s Named) GetName() string {
	return s.Name
}
func (s Named) GetNamespace() string {
	return s.Namespace
}

type Labeled struct {
	Labels map[string]string
}

func (l Labeled) GetLabels() map[string]string {
	return l.Labels
}

func NewLabeled(n map[string]string) Labeled {
	return Labeled{n}
}

func NewNamed(n Namer) Named {
	return Named{
		Namespace: n.GetNamespace(),
		Name:      n.GetName(),
	}
}

type Namer interface {
	GetName() string
	GetNamespace() string
}

func AssertCollectionNameOrder[T krtlite.ResourceNamer](coll krtlite.Collection[T], expectedNames ...string) func() bool {
	return func() bool {
		listed := coll.List()
		if len(listed) != len(expectedNames) {
			return false
		}
		for i, name := range expectedNames {
			if listed[i].ResourceName() != name {
				return false
			}
		}
		return true
	}
}

func AssertCollectionDeepEquals[T any](coll krtlite.Collection[T], expectedObjs ...T) func() bool {
	return func() bool {
		listed := coll.List()
		if len(listed) != len(expectedObjs) {
			return false
		}
		for i, obj := range expectedObjs {
			if !reflect.DeepEqual(obj, listed[i]) {
				return false
			}
		}
		return true
	}
}
