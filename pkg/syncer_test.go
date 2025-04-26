package pkg_test

import (
	"context"
	krtlite "github.com/kalexmills/krt-lite/pkg"
	"github.com/stretchr/testify/assert"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"sync"
	"sync/atomic"
	"testing"
)

//func init() {
//	slog.SetLogLoggerLevel(slog.LevelDebug)
//}

func TestRegistrationSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	t.Run("informer", func(t *testing.T) {
		c := fake.NewClientset(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "name",
				Namespace: "namespace",
			},
		})

		ConfigMaps := krtlite.NewTypedClientInformer[*corev1.ConfigMap](ctx, c.CoreV1().ConfigMaps(metav1.NamespaceAll))

		doTest(t, ctx, ConfigMaps)
	})

	t.Run("derivedCollection", func(t *testing.T) {
		c := fake.NewClientset(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "name",
				Namespace: "namespace",
			},
			Status: corev1.PodStatus{PodIP: "1.2.3.4"},
		})

		Pods := krtlite.NewTypedClientInformer[*corev1.Pod](ctx, c.CoreV1().Pods("namespace"))
		SimplePods := SimplePodCollection(Pods)

		doTest(t, ctx, SimplePods)
	})

	t.Run("mergedCollection", func(t *testing.T) {
		c := fake.NewClientset(
			&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "name",
					Namespace: "namespace",
				},
				Spec: PodSpecWithImages("busybox:latest"),
			},
			&batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "name",
					Namespace: "namespace",
				},
				Spec: batchv1.JobSpec{
					Template: PodTemplateSpecWithImages("busybox:latest"),
				},
			},
		)

		Pods := krtlite.NewTypedClientInformer[*corev1.Pod](ctx, c.CoreV1().Pods(metav1.NamespaceAll),
			krtlite.WithName("Pods"))
		Jobs := krtlite.NewTypedClientInformer[*batchv1.Job](ctx, c.BatchV1().Jobs(metav1.NamespaceAll),
			krtlite.WithName("Jobs"))

		Containers := krtlite.MergeDisjoint([]krtlite.Collection[Image]{SimpleImageCollectionFromJobs(Jobs), SimpleImageCollectionFromPods(Pods)},
			krtlite.WithName("Containers"))

		doTest(t, ctx, Containers)
	})
}

func doTest[T any](t *testing.T, ctx context.Context, collection krtlite.Collection[T]) {
	var (
		gotEvent  = &atomic.Uint32{}
		reg       cache.ResourceEventHandlerRegistration
		startSync sync.WaitGroup // wait group to satisfy race detector
	)

	startSync.Add(1)
	reg1Delayed := collection.RegisterBatched(func(o []krtlite.Event[T]) {
		startSync.Wait() // avoid racing for the value of reg.
		if gotEvent.Add(1) > 0 {
			return // we only assert on the first event
		}
		assert.Equal(t, false, reg.HasSynced(), "expected register handler to run at least once before sync")
	}, true)
	reg = reg1Delayed
	startSync.Done()

	ok := cache.WaitForCacheSync(ctx.Done(), reg.HasSynced)
	assert.True(t, ok)

	assert.GreaterOrEqual(t, int(gotEvent.Load()), 1, "expected register handler to run at least once")
}
