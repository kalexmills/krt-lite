package krtlite_test

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	krtlite "github.com/kalexmills/krt-lite"
	"github.com/stretchr/testify/assert"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

func init() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
}

const (
	timeout      = time.Second * 5       // timeout is used for all Eventually and context.WithTimeout calls.
	pollInterval = 20 * time.Millisecond // poll interval is used for all Eventually
)

type SimplePod struct {
	Named
	Labeled
	IP             string
	ContainerNames []string
}

func NewSimplePod(name, namespace, ip string, labels map[string]string) SimplePod {
	return SimplePod{
		Named:   Named{Name: name, Namespace: namespace},
		Labeled: NewLabeled(labels),
		IP:      ip,
	}
}

// SimplePodCollection is a collection of pods with PodIPs.
func SimplePodCollection(ctx context.Context, pods krtlite.Collection[*corev1.Pod]) krtlite.Collection[SimplePod] {
	return krtlite.Map(pods, func(ctx krtlite.Context, i *corev1.Pod) *SimplePod {
		if i.Status.PodIP == "" {
			return nil
		}
		return &SimplePod{
			Named:   NewNamed(i),
			Labeled: NewLabeled(i.Labels),
			IP:      i.Status.PodIP,
		}
	}, krtlite.WithName("SimplePods"), krtlite.WithContext(ctx))
}

type Image string

func (i Image) Key() string {
	return string(i)
}

func SimpleImageCollectionFromJobs(ctx context.Context, jobs krtlite.Collection[*batchv1.Job]) krtlite.Collection[Image] {
	return krtlite.FlatMap(jobs, func(ctx krtlite.Context, job *batchv1.Job) []Image {
		var result []Image
		for _, c := range job.Spec.Template.Spec.Containers {
			if c.Image != "" {
				result = append(result, Image(c.Image))
			}
		}
		return result
	}, krtlite.WithName("ImagesFromJobs"), krtlite.WithContext(ctx))
}

func SimpleImageCollectionFromPods(ctx context.Context, pods krtlite.Collection[*corev1.Pod]) krtlite.Collection[Image] {
	return krtlite.FlatMap(pods, func(ctx krtlite.Context, pod *corev1.Pod) []Image {
		var result []Image
		for _, c := range pod.Spec.Containers {
			if c.Image != "" {
				result = append(result, Image(c.Image))
			}
		}
		return result
	}, krtlite.WithName("ImagesFromPods"), krtlite.WithContext(ctx))
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

func (n SimpleNamespace) Key() string {
	return n.Name
}

func SimpleNamespaceCollection(ctx context.Context, pods krtlite.Collection[*corev1.Namespace]) krtlite.Collection[SimpleNamespace] {
	return krtlite.Map(pods, func(ctx krtlite.Context, i *corev1.Namespace) *SimpleNamespace {
		return &SimpleNamespace{
			Named:   NewNamed(i),
			Labeled: NewLabeled(i.Labels),
		}
	}, krtlite.WithName("SimpleNamespaces"), krtlite.WithContext(ctx))
}

type SimpleEndpoint struct {
	Pod       string
	Service   string
	Namespace string
	IP        string
}

func (s SimpleEndpoint) Key() string {
	return "/" + s.Namespace + "/" + s.Service + "/" + s.Pod
}

func SimpleEndpointsCollection(ctx context.Context, pods krtlite.Collection[SimplePod], services krtlite.Collection[SimpleService]) krtlite.Collection[SimpleEndpoint] {
	return krtlite.FlatMap[SimpleService, SimpleEndpoint](services, func(ctx krtlite.Context, svc SimpleService) []SimpleEndpoint {
		pods := krtlite.Fetch(ctx, pods, krtlite.MatchLabels(svc.Selector))
		var res []SimpleEndpoint
		for _, pod := range pods {
			res = append(res, SimpleEndpoint{
				Pod:       pod.Name,
				Service:   svc.Name,
				Namespace: svc.Namespace,
				IP:        pod.IP,
			})
		}
		return res
	}, krtlite.WithName("SimpleEndpoints"), krtlite.WithContext(ctx))
}

type SizedPod struct {
	Named
	Size string
}

func SizedPodCollection(ctx context.Context, pods krtlite.Collection[*corev1.Pod]) krtlite.Collection[SizedPod] {
	return krtlite.Map(pods, func(ctx krtlite.Context, i *corev1.Pod) *SizedPod {
		s, f := i.Labels["size"]
		if !f {
			return nil
		}
		return &SizedPod{
			Named: NewNamed(i),
			Size:  s,
		}
	}, krtlite.WithName("SizedPods"), krtlite.WithContext(ctx))
}

func ListSorted[T any](c krtlite.Collection[T]) []T {
	result := c.List()
	slices.SortFunc(result, func(a, b T) int {
		return strings.Compare(krtlite.GetKey(a), krtlite.GetKey(b))
	})
	return result
}

type tracker[T any] struct {
	t *testing.T

	mut    *sync.RWMutex
	events map[string]struct{}
}

func NewTracker[T any](t *testing.T) tracker[T] {
	return tracker[T]{
		t:      t,
		mut:    &sync.RWMutex{},
		events: make(map[string]struct{}),
	}
}

func (t *tracker[T]) Track(e krtlite.Event[T]) {
	t.mut.Lock()
	defer t.mut.Unlock()
	key := fmt.Sprintf("%v/%s", e.Type, krtlite.GetKey(e.Latest()))
	t.events[key] = struct{}{}
}

func (t *tracker[T]) Empty() {
	t.t.Helper()
	t.mut.RLock()
	defer t.mut.RUnlock()
	assert.Empty(t.t, t.events)
}

// Wait waits for events to have occurred, no order is asserted.
func (t *tracker[T]) Wait(events ...string) {
	t.t.Helper()
	assert.Eventually(t.t, func() bool {
		t.mut.Lock()
		defer t.mut.Unlock()
		for _, ev := range events {
			if _, ok := t.events[ev]; !ok {
				return false
			}
		}

		// consume events once all have been verified
		for _, ev := range events {
			delete(t.events, ev)
		}
		return true
	}, timeout, pollInterval, "expected events: %v", events)
}

type LabeledNamed struct {
	Named
	Labeled
}

type Named struct {
	Namespace string
	Name      string
}

func (s Named) Key() string {
	if s.Namespace != "" {
		return s.Namespace + "/" + s.Name
	}
	return s.Name
}
func (s Named) GetName() string {
	return s.Name
}
func (s Named) GetNamespace() string {
	return s.Namespace
}

func (s Named) ResourceName() string {
	return s.Namespace + "/" + s.Name
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

// CollectionKeysMatch lists the collection and asserts it contains only resources with the provided names.
func CollectionKeysMatch[T any](coll krtlite.Collection[T], expectedNames ...string) func() bool {
	sort.Strings(expectedNames)

	return func() bool {
		listed := ListSorted(coll)
		if len(listed) != len(expectedNames) {
			return false
		}
		for i, name := range expectedNames {
			if krtlite.GetKey(listed[i]) != name {
				return false
			}
		}
		return true
	}
}

func AssertEventuallyKeysMatch[T any](t *testing.T, coll krtlite.Collection[T], expectedNames ...string) {
	t.Helper()
	passed := assert.Eventually(t, func() bool {
		return CollectionKeysMatch(coll, expectedNames...)()
	}, timeout, pollInterval)
	if !passed {
		t.Errorf("got :%v; want: %v", ListSorted(coll), expectedNames)
	}
}

func CollectionContentsDeepEquals[T any](coll krtlite.Collection[T], expectedObjs ...T) bool {
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

func CollectionEmpty[T any](coll krtlite.Collection[T]) bool {
	return CollectionContentsDeepEquals(coll)
}

func AssertEventuallyDeepEquals[T any](t *testing.T, coll krtlite.Collection[T], expectedObjs ...T) {
	t.Helper()
	passed := assert.Eventually(t, func() bool {
		return CollectionContentsDeepEquals(coll, expectedObjs...)
	}, timeout, pollInterval)
	if !passed {
		t.Errorf("got: %v; want: %v", coll.List(), expectedObjs)
	}
}

func PodTemplateSpecWithImages(images ...string) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		Spec: PodSpecWithImages(images...),
	}
}

func PodSpecWithImages(images ...string) corev1.PodSpec {
	var containers = make([]corev1.Container, 0, len(images))
	for _, image := range images {
		containers = append(containers, corev1.Container{Image: image})
	}
	return corev1.PodSpec{
		Containers: containers,
	}
}

func AssertEventually(t *testing.T, f func() bool, msgAndArgs ...any) {
	t.Helper()
	assert.Eventually(t, f, timeout, pollInterval, msgAndArgs...)
}

func AssertEventuallyEqual(t *testing.T, expected any, getActual func() any, msgAndArgs ...any) {
	t.Helper()
	passed := assert.Eventually(t, func() bool {
		actual := getActual()
		return reflect.DeepEqual(expected, actual)
	}, timeout, pollInterval, msgAndArgs...)

	if !passed {
		t.Errorf("want: %v; got: %v", expected, getActual())
	}
}
