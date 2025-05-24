package krtlite_test

import (
	"context"
	krtlite "github.com/kalexmills/krt-lite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"slices"
	"testing"
)

type SimpleService struct {
	Named
	Selector map[string]string
}

func SimpleServiceCollection(ctx context.Context, services krtlite.Collection[*corev1.Service]) krtlite.Collection[SimpleService] {
	return krtlite.Map(services, func(ctx krtlite.Context, i *corev1.Service) *SimpleService {
		return &SimpleService{
			Named:    NewNamed(i),
			Selector: i.Spec.Selector,
		}
	}, krtlite.WithName("SimpleService"), krtlite.WithContext(ctx))
}

func TestDerivedCollectionSimple(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), timeout*5)
	defer cancel()

	client := fake.NewClientset()
	nsClient := client.CoreV1().Namespaces()

	// create collections
	Namespaces := krtlite.NewTypedClientInformer[*corev1.Namespace, *corev1.NamespaceList](ctx, nsClient,
		krtlite.WithContext(ctx))
	Namespaces.WaitUntilSynced(ctx.Done())
	SimpleNamespaces := SimpleNamespaceCollection(ctx, Namespaces)

	assert.Empty(t, ListSorted(SimpleNamespaces), "expected collection to start empty")

	// add namespace
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns-1"}}
	ns, err := nsClient.Create(ctx, ns, metav1.CreateOptions{})
	require.NoError(t, err)

	AssertEventually(t, CollectionKeysMatch(SimpleNamespaces, "ns-1"),
		"expected collection to sync")

	// add labels to namespace and assert update is seen
	ns.Labels = map[string]string{
		"foo": "bar",
	}

	_, err = nsClient.Update(ctx, ns, metav1.UpdateOptions{})
	require.NoError(t, err)

	AssertEventuallyDeepEquals(t, SimpleNamespaces, NewSimpleNamespace("ns-1", map[string]string{"foo": "bar"}))

	// modify labels and assert we see updates
	ns.Labels["foo"] = "baz-updated"

	_, err = nsClient.Update(ctx, ns, metav1.UpdateOptions{})
	require.NoError(t, err)

	AssertEventuallyDeepEquals(t, SimpleNamespaces, NewSimpleNamespace("ns-1", map[string]string{"foo": "baz-updated"}))

	// add a new handler and assert it gets synced.
	tt := NewTracker[SimpleNamespace](t)
	SimpleNamespaces.Register(tt.Track)
	tt.Wait("add/ns-1")

	// delete and assert we see deletion events
	err = nsClient.Delete(ctx, ns.Name, metav1.DeleteOptions{})
	require.NoError(t, err)
	tt.Wait("delete/ns-1")
}

func TestDerivedCollectionInitialState(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), timeout*5)
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
	pods := krtlite.NewTypedClientInformer[*corev1.Pod](ctx, c.CoreV1().Pods("namespace"),
		krtlite.WithName("Pods"), krtlite.WithContext(ctx))
	services := krtlite.NewTypedClientInformer[*corev1.Service](ctx, c.CoreV1().Services("namespace"),
		krtlite.WithName("Services"), krtlite.WithContext(ctx))

	// assert that collections are equal immediately after waiting for sync.
	SimplePods := SimplePodCollection(ctx, pods)
	SimpleServices := SimpleServiceCollection(ctx, services)
	SimpleEndpoints := SimpleEndpointsCollection(ctx, SimplePods, SimpleServices)
	assert.True(t, SimpleEndpoints.WaitUntilSynced(ctx.Done()))

	assert.Equal(t, []SimpleEndpoint{{Pod: "pod", Service: "svc", Namespace: "namespace", IP: "1.2.3.4"}},
		ListSorted(SimpleEndpoints))
}

func TestCollectionMerged(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), timeout*5)
	defer cancel()

	c := fake.NewClientset()
	podClient := c.CoreV1().Pods("namespace")
	pods := krtlite.NewTypedClientInformer[*corev1.Pod](ctx, podClient,
		krtlite.WithName("Pods"), krtlite.WithContext(ctx))
	svcClient := c.CoreV1().Services("namespace")
	services := krtlite.NewTypedClientInformer[*corev1.Service](ctx, svcClient,
		krtlite.WithName("Services"), krtlite.WithContext(ctx))

	SimplePods := SimplePodCollection(ctx, pods)
	SimpleServices := SimpleServiceCollection(ctx, services)
	SimpleEndpoints := SimpleEndpointsCollection(ctx, SimplePods, SimpleServices)
	SimpleEndpoints.WaitUntilSynced(ctx.Done())

	assert.Empty(t, ListSorted(SimpleEndpoints))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod",
			Namespace: "namespace",
			Labels:    map[string]string{"app": "foo"},
		},
	}
	pod, err := podClient.Create(ctx, pod, metav1.CreateOptions{})

	require.NoError(t, err)
	assert.Empty(t, ListSorted(SimpleEndpoints), "expected no endpoints")

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc",
			Namespace: "namespace",
		},
		Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "foo"}},
	}
	svc, err = svcClient.Create(ctx, svc, metav1.CreateOptions{})

	require.NoError(t, err)
	assert.Empty(t, ListSorted(SimpleEndpoints), "expected no endpoints")

	pod.Status = corev1.PodStatus{PodIP: "1.2.3.4"}
	pod, err = podClient.Update(ctx, pod, metav1.UpdateOptions{})
	require.NoError(t, err)

	AssertEventuallyDeepEquals(t, SimpleEndpoints,
		SimpleEndpoint{Pod: "pod", Service: "svc", Namespace: "namespace", IP: "1.2.3.4"},
	)

	pod.Status.PodIP = "1.2.3.5"
	pod, err = podClient.UpdateStatus(ctx, pod, metav1.UpdateOptions{})
	require.NoError(t, err)

	AssertEventuallyDeepEquals(t, SimpleEndpoints,
		SimpleEndpoint{Pod: "pod", Service: "svc", Namespace: "namespace", IP: "1.2.3.5"},
	)

	err = podClient.Delete(ctx, pod.Name, metav1.DeleteOptions{})
	require.NoError(t, err)

	AssertEventuallyDeepEquals(t, SimpleEndpoints)

	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "name2",
			Namespace: "namespace",
			Labels:    map[string]string{"app": "foo"},
		},
		Status: corev1.PodStatus{PodIP: "2.3.4.5"},
	}
	_, err = podClient.Create(ctx, pod, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = podClient.Create(ctx, pod2, metav1.CreateOptions{})
	require.NoError(t, err)

	AssertEventuallyDeepEquals(t, SimpleEndpoints,
		SimpleEndpoint{pod2.Name, svc.Name, pod2.Namespace, pod2.Status.PodIP},
		SimpleEndpoint{pod.Name, svc.Name, pod.Namespace, pod.Status.PodIP},
	)
}

func TestCollectionDiamond(t *testing.T) {
	// Tests a diamond dependency graph -- each edge has direction pointing downwards.
	//
	//           Pods
	//          /    \
	// SimplePods    SizedPods
	//          \    /
	//       PodSizeCount
	//
	type PodSizeCount struct {
		Named
		MatchingSizes int
	}

	ctx, cancel := context.WithTimeout(t.Context(), timeout*5)
	defer cancel()

	c := fake.NewClientset()
	podClient := c.CoreV1().Pods("namespace")

	Pods := krtlite.NewTypedClientInformer[*corev1.Pod](ctx, c.CoreV1().Pods("namespace"),
		krtlite.WithName("Pods"), krtlite.WithContext(ctx))

	SimplePods := SimplePodCollection(ctx, Pods)
	SizedPods := SizedPodCollection(ctx, Pods)

	PodSizeCounts := krtlite.Map[SimplePod, PodSizeCount](SimplePods, func(ctx krtlite.Context, pd SimplePod) *PodSizeCount {
		if _, f := pd.Labels["want-size"]; !f {
			return nil
		}

		matches := krtlite.Fetch(ctx, SizedPods, krtlite.MatchFilter(func(p SizedPod) bool {
			return p.Size == pd.Labels["want-size"]
		}))

		return &PodSizeCount{
			Named:         pd.Named,
			MatchingSizes: len(matches),
		}
	}, krtlite.WithName("PodSizeCounts"), krtlite.WithContext(ctx))

	PodSizeCounts.WaitUntilSynced(ctx.Done())

	tt := NewTracker[PodSizeCount](t)
	PodSizeCounts.Register(tt.Track).WaitUntilSynced(ctx.Done())

	assert.Empty(t, ListSorted(PodSizeCounts))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "name",
			Namespace: "namespace",
			Labels:    map[string]string{"want-size": "large"},
		},
		Status: corev1.PodStatus{PodIP: "1.2.3.4"},
	}
	pod, err := podClient.Create(ctx, pod, metav1.CreateOptions{})
	require.NoError(t, err)

	tt.Wait("add/namespace/name")
	expected := []PodSizeCount{{
		Named:         NewNamed(pod),
		MatchingSizes: 0,
	}}
	assert.Equal(t, expected, ListSorted(PodSizeCounts))

	largePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "name-large",
			Namespace: "namespace",
			Labels:    map[string]string{"size": "large"},
		},
		Status: corev1.PodStatus{PodIP: "1.2.3.5"},
	}

	largePod, err = podClient.Create(ctx, largePod, metav1.CreateOptions{})
	require.NoError(t, err)

	tt.Wait("update/namespace/name")

	assert.Equal(t, []PodSizeCount{{
		Named:         NewNamed(pod),
		MatchingSizes: 1,
	}}, ListSorted(PodSizeCounts))

	smallPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "name-small",
			Namespace: "namespace",
			Labels:    map[string]string{"size": "small"},
		},
		Status: corev1.PodStatus{PodIP: "1.2.3.6"},
	}

	_, err = podClient.Create(ctx, smallPod, metav1.CreateOptions{})
	require.NoError(t, err)

	largePod, err = podClient.Update(ctx, largePod, metav1.UpdateOptions{})
	require.NoError(t, err)

	expected = []PodSizeCount{{
		Named:         NewNamed(pod),
		MatchingSizes: 1,
	}}
	assert.Equal(t, expected, ListSorted(PodSizeCounts))

	tt.Empty()

	largePod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "name-large2",
			Namespace: "namespace",
			Labels:    map[string]string{"size": "large"},
		},
		Status: corev1.PodStatus{PodIP: "1.2.3.7"},
	}

	largePod2, err = podClient.Create(ctx, largePod2, metav1.CreateOptions{})
	require.NoError(t, err)
	expected = []PodSizeCount{{
		Named:         NewNamed(pod),
		MatchingSizes: 1,
	}}
	assert.Equal(t, expected, ListSorted(PodSizeCounts))

	dual := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "name-dual",
			Namespace: "namespace",
			Labels:    map[string]string{"size": "large", "want-size": "small"},
		},
		Status: corev1.PodStatus{PodIP: "1.2.3.8"},
	}

	dual, err = podClient.Create(ctx, dual, metav1.CreateOptions{})
	require.NoError(t, err)
	tt.Wait("update/namespace/name", "add/namespace/name-dual")

	expected = []PodSizeCount{
		{
			Named:         NewNamed(pod),
			MatchingSizes: 3,
		},
		{
			Named:         NewNamed(dual),
			MatchingSizes: 1,
		},
	}
	assert.Equal(t, expected, ListSorted(PodSizeCounts))

	largePod2.Labels["size"] = "small"
	_, err = podClient.Update(ctx, largePod2, metav1.UpdateOptions{})
	require.NoError(t, err)

	tt.Wait("update/namespace/name-dual", "update/namespace/name")
	expected = []PodSizeCount{
		{
			Named:         NewNamed(pod),
			MatchingSizes: 2,
		},
		{
			Named:         NewNamed(dual),
			MatchingSizes: 2,
		},
	}
	assert.Equal(t, expected, ListSorted(PodSizeCounts))

	err = podClient.Delete(ctx, dual.Name, metav1.DeleteOptions{})
	require.NoError(t, err)

	tt.Wait("update/namespace/name", "delete/namespace/name-dual")
	expected = []PodSizeCount{{
		Named:         NewNamed(pod),
		MatchingSizes: 1,
	}}
	assert.Equal(t, expected, ListSorted(PodSizeCounts))

	err = podClient.Delete(ctx, largePod.Name, metav1.DeleteOptions{})
	require.NoError(t, err)

	tt.Wait("update/namespace/name")
	expected = []PodSizeCount{{
		Named:         NewNamed(pod),
		MatchingSizes: 0,
	}}
	assert.Equal(t, expected, ListSorted(PodSizeCounts))

	err = podClient.Delete(ctx, pod.Name, metav1.DeleteOptions{})
	require.NoError(t, err)

	tt.Wait("delete/namespace/name")

	assert.Empty(t, ListSorted(PodSizeCounts))
}

func TestDerivedCollectionMultipleFetch(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	type Result struct {
		Named
		Configs []string
	}

	c := fake.NewClientset()
	podClient := c.CoreV1().Pods("namespace")
	cmClient := c.CoreV1().ConfigMaps("namespace")

	Pods := krtlite.NewTypedClientInformer[*corev1.Pod, *corev1.PodList](ctx, podClient,
		krtlite.WithName("Pods"), krtlite.WithContext(ctx))
	ConfigMaps := krtlite.NewTypedClientInformer[*corev1.ConfigMap, *corev1.ConfigMapList](ctx, cmClient,
		krtlite.WithName("ConfigMaps"), krtlite.WithContext(ctx))

	lblFoo := map[string]string{"app": "foo"}
	lblBar := map[string]string{"app": "bar"}

	Results := krtlite.Map(Pods, func(ctx krtlite.Context, i *corev1.Pod) *Result {
		var names []string

		foos := krtlite.Fetch(ctx, ConfigMaps, krtlite.MatchLabels(lblFoo))
		for _, cm := range foos {
			names = append(names, cm.Name)
		}

		bars := krtlite.Fetch(ctx, ConfigMaps, krtlite.MatchLabels(lblBar))
		for _, cm := range bars {
			names = append(names, cm.Name)
		}

		slices.Sort(names)
		return &Result{
			Named:   NewNamed(i),
			Configs: names,
		}
	}, krtlite.WithName("Results"), krtlite.WithContext(ctx))

	Results.WaitUntilSynced(ctx.Done())

	assert.Empty(t, ListSorted(Results))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "name",
			Namespace: "namespace",
		},
	}

	assertEventuallyLabelsEqual := func(labels ...string) {
		t.Helper()
		AssertEventuallyDeepEquals(t, Results, Result{Named: NewNamed(pod), Configs: labels})
	}

	pod, err := podClient.Create(ctx, pod, metav1.CreateOptions{})
	require.NoError(t, err)
	assertEventuallyLabelsEqual()

	_, err = cmClient.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "foo1", Labels: lblFoo}}, metav1.CreateOptions{})
	require.NoError(t, err)
	assertEventuallyLabelsEqual("foo1")

	_, err = cmClient.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "switch", Labels: lblFoo}}, metav1.CreateOptions{})
	require.NoError(t, err)
	assertEventuallyLabelsEqual("foo1", "switch")

	_, err = cmClient.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "bar1", Labels: lblBar}}, metav1.CreateOptions{})
	require.NoError(t, err)
	assertEventuallyLabelsEqual("bar1", "foo1", "switch")

	_, err = cmClient.Update(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "switch", Labels: lblBar}}, metav1.UpdateOptions{})
	require.NoError(t, err)
	assertEventuallyLabelsEqual("bar1", "foo1", "switch")

	_, err = cmClient.Update(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "switch", Labels: nil}}, metav1.UpdateOptions{})
	require.NoError(t, err)
	assertEventuallyLabelsEqual("bar1", "foo1")

	err = cmClient.Delete(ctx, "bar1", metav1.DeleteOptions{})
	require.NoError(t, err)
	assertEventuallyLabelsEqual("foo1")

	err = cmClient.Delete(ctx, "foo1", metav1.DeleteOptions{})
	require.NoError(t, err)
	assertEventuallyLabelsEqual()
}

func TestDerivedCollectionSync(t *testing.T) {

}
