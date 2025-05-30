package krtlite_test

import (
	"context"
	"fmt"
	krtlite "github.com/kalexmills/krt-lite"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"log/slog"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
	"time"
)

// TestDetectDroppedEvents ensures that events are not dropped at larger scales. Essentially the Benchmark with
// assertions -- during development, we noticed events were being "dropped" due to deduplication. This test exists to
// ensure we don't drop events via another bug.
func TestDetectDroppedEvents(t *testing.T) {
	const (
		N       = 100
		K       = 50 // K cannot be higher than watch.DefaultChanSize, which can't be set with the race detector enabled
		timeout = 10 * time.Second
	)

	oldLevel := slog.SetLogLoggerLevel(slog.LevelWarn)
	defer slog.SetLogLoggerLevel(oldLevel)

	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	c := fake.NewFakeClient()

	var initialPods []*corev1.Pod
	for i := 0; i < K; i++ {
		initialPods = append(initialPods, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("pod-%d", i),
				Namespace: fmt.Sprintf("ns-%d", i%2),
				Labels: map[string]string{
					"app": fmt.Sprintf("app-%d", i%25),
				},
			},
			Spec: corev1.PodSpec{
				ServiceAccountName: "fake-sa",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: GetIP(),
			},
		})
	}

	var initialServices []*corev1.Service
	for i := 0; i < 50; i++ {
		initialServices = append(initialServices, &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("pod-%d", i),
				Namespace: fmt.Sprintf("ns-%d", i%2),
			},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{
					"app": fmt.Sprintf("app-%d", i%25),
				},
			},
		})
	}

	events := make(chan string, K)

	Pods := krtlite.NewInformer[*corev1.Pod, corev1.PodList](ctx, c,
		krtlite.WithName("Pods"), krtlite.WithContext(ctx))
	Services := krtlite.NewInformer[*corev1.Service, corev1.ServiceList](ctx, c,
		krtlite.WithName("Services"), krtlite.WithContext(ctx))
	ServicesByNamespace := krtlite.NewNamespaceIndex(Services)

	Workloads := krtlite.Map(Pods, func(ktx krtlite.Context, p *corev1.Pod) *Workload {
		if p.Status.PodIP == "" {
			return nil
		}

		services := krtlite.Fetch(ktx, Services, krtlite.MatchIndex(ServicesByNamespace, p.Namespace),
			krtlite.MatchSelectsLabels(p.Labels, krtlite.ExtractPodSelector))

		result := &Workload{
			Named: NewNamed(p),
			IP:    p.Status.PodIP,
		}
		for _, service := range services {
			result.ServiceNames = append(result.ServiceNames, service.Name)
		}
		return result
	}, krtlite.WithName("Workloads"), krtlite.WithContext(ctx))

	reg := Workloads.Register(func(e krtlite.Event[Workload]) {
		events <- fmt.Sprintf("%s-%s", e.Latest().Name, e.Type)
	})

	sentCount := 0

	reg.WaitUntilSynced(ctx.Done())

	for _, pod := range initialPods {
		_ = c.Create(ctx, pod)
		sentCount++
	}

	for _, s := range initialServices {
		_ = c.Create(ctx, s)
	}

	var lastEventSeen time.Time
	count := 0

	drain := func() {
		// drain K updates from the queue, if fewer than exactly K updates were sent, this will force a deadlock
		for i := 0; i < K; i++ {
			select {
			case <-events:
				count++
				lastEventSeen = time.Now()
			case <-ctx.Done():
				// panic to force a stack trace. If you do not see one, run go test with GOTRACEBACK=all.
				panic(fmt.Sprintf("system deadlocked after %s: received %d events; sent %d; last event seen %s ago",
					timeout, count, sentCount, time.Since(lastEventSeen).String()))
			}
		}
	}

	drain()

	for n := 0; n < N; n++ {
		for i := 0; i < K; i++ { // send K updates
			p := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("pod-%d", i),
					Namespace: fmt.Sprintf("ns-%d", i%2),
					Labels: map[string]string{
						"app": fmt.Sprintf("app-%d", i%25),
					},
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: "fake-sa",
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					PodIP: GetIP(),
				},
			}
			_ = c.Status().Update(ctx, p.DeepCopy()) // deepCopy to prevent client from overwriting fields
			_ = c.Update(ctx, p)
			sentCount++
		}
		drain()
	}

	assert.Equal(t, sentCount, count, "send and receive counts to not match")
}
