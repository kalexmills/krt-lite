package krt_lite_test

import (
	"context"
	krtlite "github.com/kalexmills/krt-lite"
	"github.com/stretchr/testify/assert"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sync/atomic"
	"testing"
)

func TestMergeDisjoint(t *testing.T) {
	c1 := krtlite.NewSingleton[Named](nil, true)
	c2 := krtlite.NewSingleton[Named](nil, true)
	c3 := krtlite.NewSingleton[Named](nil, true)

	j := krtlite.MergeDisjoint([]krtlite.Collection[Named]{c1, c2, c3})

	last := atomic.Value{}
	last.Store("")

	lastEquals := func(str string) func() bool {
		return func() bool {
			return last.Load() == str
		}
	}
	j.Register(func(o krtlite.Event[Named]) {
		last.Store(o.Latest().Key())
	})

	AssertEventually(t, lastEquals(""))

	c1.Set(&Named{Name: "c1", Namespace: "a"})
	AssertEventually(t, lastEquals("a/c1"))

	c2.Set(&Named{Name: "c2", Namespace: "a"})
	AssertEventually(t, lastEquals("a/c2"))

	c3.Set(&Named{Name: "c3", Namespace: "a"})
	AssertEventually(t, lastEquals("a/c3"))

	c1.Set(&Named{Name: "c1", Namespace: "b"})
	AssertEventually(t, lastEquals("b/c1"))

	assert.True(t, CollectionKeysMatch(j, "b/c1", "a/c3", "a/c2")())
}

func TestMergeDisjointSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	c := fake.NewClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "name",
				Namespace: "namespace",
			},
			Spec: PodSpecWithImages("cilium:latest"),
		},
		&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "name",
				Namespace: "namespace",
			},
			Spec: batchv1.JobSpec{
				Template: PodTemplateSpecWithImages("nikola/netshoot:latest"),
			},
		},
	)

	Pods := krtlite.NewTypedClientInformer[*corev1.Pod](ctx, c.CoreV1().Pods(metav1.NamespaceAll))
	Jobs := krtlite.NewTypedClientInformer[*batchv1.Job](ctx, c.BatchV1().Jobs(metav1.NamespaceAll))
	PodImages := SimpleImageCollectionFromPods(Pods)
	JobImages := SimpleImageCollectionFromJobs(Jobs)

	AllImages := krtlite.MergeDisjoint([]krtlite.Collection[Image]{PodImages, JobImages})

	assert.True(t, AllImages.WaitUntilSynced(ctx.Done()))
	assert.True(t, CollectionKeysMatch(AllImages, "cilium:latest", "nikola/netshoot:latest")())
}
