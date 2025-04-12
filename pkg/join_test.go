package pkg_test

import (
	"context"
	"sync/atomic"

	krtlite "github.com/kalexmills/krt-plusplus/pkg"
	"github.com/stretchr/testify/assert"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
)

func TestJoinCollection(t *testing.T) {
	c1 := krtlite.NewSingleton[Named](nil, true)
	c2 := krtlite.NewSingleton[Named](nil, true)
	c3 := krtlite.NewSingleton[Named](nil, true)

	j := krtlite.JoinDisjoint([]krtlite.Collection[Named]{c1, c2, c3})

	last := atomic.Value{}
	last.Store("")

	lastEquals := func(str string) func() bool {
		return func() bool {
			return last.Load() == str
		}
	}
	j.Register(func(o krtlite.Event[Named]) {
		last.Store(o.Latest().ResourceName())
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

func TestJoin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := fake.NewClientset()
	podClient := c.CoreV1().Pods("namespace")
	jobClient := c.BatchV1().Jobs("namespace")

	Pods := krtlite.NewInformer[*corev1.Pod](ctx, c.CoreV1().Pods("namespace"))
	Jobs := krtlite.NewInformer[*batchv1.Job](ctx, c.BatchV1().Jobs("namespace"))
	PodImages := SimpleImageCollectionFromPods(Pods)
	JobImages := SimpleImageCollectionFromJobs(Jobs)

	joiner := func(vals []Image) Image {
		return vals[0]
	}
	Images := krtlite.Join[Image]([]krtlite.Collection[Image]{PodImages, JobImages}, joiner)

	Images.WaitUntilSynced(ctx.Done())

	assert.Empty(t, Images.List(), "expected join collection to start empty")

	// create a pod with no images and expect join collection to be empty
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "name",
			Namespace: "namespace",
		},
	}
	_, err := podClient.Create(ctx, pod, metav1.CreateOptions{})
	assert.NoError(t, err)

	assert.Empty(t, Images.List(), "expected join collection to be empty after ineffective updates upstream")

	// create a job and update a pod and expect both images to propagate
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "name",
			Namespace: "namespace",
		},
		Spec: batchv1.JobSpec{
			Template: PodTemplateSpecWithImages("istio:latest"),
		},
	}
	job, err = jobClient.Create(ctx, job, metav1.CreateOptions{})
	assert.NoError(t, err)

	pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "name",
			Namespace: "namespace",
		},
		Spec: PodSpecWithImages("nikola/netshoot:latest"),
	}
	_, err = podClient.Update(ctx, pod, metav1.UpdateOptions{})
	assert.NoError(t, err)

	AssertEventually(t, CollectionKeysMatch(Images, "istio:latest", "nikola/netshoot:latest"),
		"expected join collection to eventually contain data from both sources")

	// remove containers from the job and expect result to be deleted.
	job.Spec.Template.Spec.Containers = nil
	_, err = jobClient.Update(ctx, job, metav1.UpdateOptions{})
	assert.NoError(t, err)

	AssertEventually(t, CollectionKeysMatch(Images, "nikola/netshoot:latest"),
		"expected join collection entries to eventually be removed when upstream updated")

	// delete pod and expect no images.
	err = podClient.Delete(ctx, pod.Name, metav1.DeleteOptions{})
	assert.NoError(t, err)

	AssertEventually(t, CollectionContentsDeepEquals(Images),
		"expected join collection to eventually be empty after all sources removed")

	// ensure duplicate keys get merged
	job.Spec.Template = PodTemplateSpecWithImages("nikola/netshoot:latest")

	_, err = jobClient.Update(ctx, job, metav1.UpdateOptions{})
	assert.NoError(t, err)

	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "name-2",
			Namespace: "namespace",
		},
		Spec: PodSpecWithImages("nikola/netshoot:latest"),
	}

	_, err = podClient.Create(ctx, pod2, metav1.CreateOptions{})
	assert.NoError(t, err)

	AssertEventually(t, CollectionKeysMatch(Images, "nikola/netshoot:latest"),
		"expected join collection to eventually overlap duplicate keys correctly")

	// ensure many entries propagate correctly
	pod.Spec = PodSpecWithImages("istio:latest")

	_, err = podClient.Create(ctx, pod, metav1.CreateOptions{})
	assert.NoError(t, err)

	pod3 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "name-3",
			Namespace: "namespace",
		},
		Spec: PodSpecWithImages("busybox:latest", "kube-api-server:v1.21.0", "istio-sidecar:latest"),
	}
	_, err = podClient.Create(ctx, pod3, metav1.CreateOptions{})
	assert.NoError(t, err)

	AssertEventually(t, CollectionKeysMatch(Images, "nikola/netshoot:latest", "istio:latest", "busybox:latest", "kube-api-server:v1.21.0", "istio-sidecar:latest"),
		"expected join collection to contain all containers")
}

func TestCollectionJoinSync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
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

	Pods := krtlite.NewInformer[*corev1.Pod](ctx, c.CoreV1().Pods(metav1.NamespaceAll))
	Jobs := krtlite.NewInformer[*batchv1.Job](ctx, c.BatchV1().Jobs(metav1.NamespaceAll))
	PodImages := SimpleImageCollectionFromPods(Pods)
	JobImages := SimpleImageCollectionFromJobs(Jobs)

	AllImages := krtlite.JoinDisjoint([]krtlite.Collection[Image]{PodImages, JobImages})

	assert.True(t, AllImages.WaitUntilSynced(ctx.Done()))
	assert.True(t, CollectionKeysMatch(AllImages, "cilium:latest", "nikola/netshoot:latest")())
}

func TestJoinJoiner(t *testing.T) {
	c1 := krtlite.NewSingleton[Named](nil, true)
	c2 := krtlite.NewSingleton[Named](nil, true)
	c3 := krtlite.NewSingleton[Named](nil, true)

	j := krtlite.Join[Named]([]krtlite.Collection[Named]{c1, c2, c3},
		func(ts []Named) Named {
			var result string
			for i, t := range ts {
				if i != 0 {
					result += "/"
				}
				result += t.Name
			}
			return Named{Name: result}
		},
	)

	c1.Set(&Named{Name: "abc"})
	AssertEventually(t, CollectionKeysMatch(j, "abc"))
	assert.NotNil(t, j.GetKey("abc"))

	c2.Set(&Named{Name: "abc"})
	AssertEventually(t, CollectionKeysMatch(j, "abc/abc"))
	assert.Equal(t, "abc/abc", j.GetKey("abc").Name)

	c3.Set(&Named{Name: "abc"})
	AssertEventually(t, CollectionKeysMatch(j, "abc/abc/abc"))
	assert.Equal(t, "abc/abc/abc", j.GetKey("abc").Name)
}
