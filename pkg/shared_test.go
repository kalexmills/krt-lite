package pkg_test

import (
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	krtlite "github.com/kalexmills/krt-plusplus/pkg"
	"github.com/stretchr/testify/assert"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

const (
	timeout      = time.Second * 3       // timeout is used for all Eventually and context.WithTimeout calls.
	pollInterval = 50 * time.Millisecond // poll interval is used for all Eventually
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
	}, krtlite.WithName("SimplePods"))
}

type Image string

func (i Image) ResourceName() string {
	return string(i)
}

func SimpleImageCollectionFromJobs(jobs krtlite.Collection[*batchv1.Job]) krtlite.Collection[Image] {
	return krtlite.FlatMap(jobs, func(job *batchv1.Job) []Image {
		var result []Image
		for _, c := range job.Spec.Template.Spec.Containers {
			if c.Image != "" {
				result = append(result, Image(c.Image))
			}
		}
		return result
	}, krtlite.WithName("ImagesFromJobs"))
}

func SimpleImageCollectionFromPods(pods krtlite.Collection[*corev1.Pod]) krtlite.Collection[Image] {
	return krtlite.FlatMap(pods, func(pod *corev1.Pod) []Image {
		var result []Image
		for _, c := range pod.Spec.Containers {
			if c.Image != "" {
				result = append(result, Image(c.Image))
			}
		}
		return result
	}, krtlite.WithName("ImagesFromPods"))
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
	}, krtlite.WithName("SimpleNamespaces"))
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
	key := fmt.Sprintf("%v/%s", e.Event, krtlite.GetKey(e.Latest()))
	t.events[key] = struct{}{}
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

func CollectionContentsDeepEquals[T any](coll krtlite.Collection[T], expectedObjs ...T) func() bool {
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

func AssertCollectionEmpty[T any](coll krtlite.Collection[T]) func() bool {
	return CollectionContentsDeepEquals(coll) // asserts empty via length check.
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
	assert.Eventually(t, func() bool {
		actual := getActual()
		return reflect.DeepEqual(expected, actual)
	}, timeout, pollInterval, msgAndArgs...)
}
