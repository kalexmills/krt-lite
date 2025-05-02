package pkg_test

import (
	"context"
	"fmt"
	krtlite "github.com/kalexmills/krt-lite/pkg"
	"istio.io/istio/pkg/kube"
	"istio.io/istio/pkg/kube/kclient/clienttest"
	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/slices"
	"istio.io/istio/pkg/test"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/watch"
	"log/slog"
	"net"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

type Workload struct {
	Named
	ServiceNames []string
	IP           string
}

type krtClientWrapper struct {
	pods clienttest.TestWriter[*corev1.Pod]
	svcs clienttest.TestWriter[*corev1.Service]
}

func (c *krtClientWrapper) CreatePod(ctx context.Context, pod *corev1.Pod) {
	c.pods.Create(pod)
}

func (c *krtClientWrapper) CreateService(ctx context.Context, svc *corev1.Service) {
	c.svcs.Create(svc)
}

func (c *krtClientWrapper) UpdatePod(ctx context.Context, pod *corev1.Pod) {
	c.pods.Update(pod)
}

type ServiceWrapper struct{ *corev1.Service }

func (s ServiceWrapper) GetLabelSelector() map[string]string {
	return s.Spec.Selector
}

func KrtController(b *testing.B, events chan string) (Client, func()) {
	c := kube.NewFakeClient()
	Pods := krt.NewInformer[*corev1.Pod](c)
	Services := krt.NewInformer[*corev1.Service](c, krt.WithObjectAugmentation(func(o any) any {
		return ServiceWrapper{o.(*corev1.Service)}
	}))
	ServicesByNamespace := krt.NewNamespaceIndex(Services)

	Workloads := krt.NewCollection(Pods, func(ctx krt.HandlerContext, p *corev1.Pod) *Workload {
		if p.Status.PodIP == "" {
			return nil
		}
		services := krt.Fetch(ctx, Services, krt.FilterIndex(ServicesByNamespace, p.Namespace), krt.FilterSelectsNonEmpty(p.GetLabels()))
		return &Workload{
			Named:        NewNamed(p),
			IP:           p.Status.PodIP,
			ServiceNames: slices.Map(services, func(e *corev1.Service) string { return e.Name }),
		}
	})
	Workloads.Register(func(e krt.Event[Workload]) {
		events <- fmt.Sprintf(e.Latest().Name, e.Event)
	})
	wrapped := &krtClientWrapper{
		pods: clienttest.NewWriter[*corev1.Pod](b, c),
		svcs: clienttest.NewWriter[*corev1.Service](b, c),
	}
	waitForSync := func() {
		c.RunAndWait(test.NewStop(b))
	}
	return wrapped, waitForSync
}

type krtliteWrapper struct {
	client client.WithWatch
}

func (k *krtliteWrapper) CreatePod(ctx context.Context, pod *corev1.Pod) {
	_ = k.client.Create(ctx, pod)
}

func (k *krtliteWrapper) CreateService(ctx context.Context, svc *corev1.Service) {
	_ = k.client.Create(ctx, svc)
}

func (k *krtliteWrapper) UpdatePod(ctx context.Context, pod *corev1.Pod) {
	_ = k.client.Update(ctx, pod)
}

func KrtLiteController(b *testing.B, events chan string) (Client, func()) {
	ctx := b.Context()

	c := fake.NewFakeClient()
	Pods := krtlite.NewInformer[*corev1.Pod, corev1.PodList](ctx, c, krtlite.WithName("Pods"))
	Services := krtlite.NewInformer[*corev1.Service, corev1.ServiceList](ctx, c, krtlite.WithName("Services"))
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
			if labels.Set(service.Labels).AsSelector().Matches(labels.Set(p.Labels)) {
				result.ServiceNames = append(result.ServiceNames, service.Name)
			}
		}
		return result
	}, krtlite.WithName("Workloads"), krtlite.WithSpuriousUpdates())

	reg := Workloads.Register(func(e krtlite.Event[Workload]) {
		events <- fmt.Sprintf("%s-%s", e.Latest().Name, e.Event)
	})

	return &krtliteWrapper{client: c}, func() {
		reg.WaitUntilSynced(ctx.Done())
	}
}

func BenchmarkController(b *testing.B) {
	oldLevel := slog.SetLogLoggerLevel(slog.LevelWarn)
	defer slog.SetLogLoggerLevel(oldLevel)
	watch.DefaultChanSize = 100_000

	benchmark := func(b *testing.B, fn func(b *testing.B, events chan string) (Client, func())) {
		slogLevel := slog.SetLogLoggerLevel(slog.LevelWarn)
		defer slog.SetLogLoggerLevel(slogLevel)

		b.Logf("starting controller")
		var initialPods []*corev1.Pod
		for i := 0; i < 1000; i++ {
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

		ctx := b.Context()
		events := make(chan string, 1000)

		c, wait := fn(b, events)
		for _, pod := range initialPods {
			c.CreatePod(ctx, pod)
		}
		for _, s := range initialServices {
			c.CreateService(ctx, s)
		}

		wait()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			for i := 0; i < 1000; i++ {
				c.UpdatePod(ctx, &corev1.Pod{
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
			drainN(events, 1000)
		}
	}

	b.Run("krt", func(b *testing.B) {
		benchmark(b, KrtController)
	})
	b.Run("krt-lite", func(b *testing.B) {
		benchmark(b, KrtLiteController)
	})
}

type Client interface {
	CreatePod(ctx context.Context, pod *corev1.Pod)
	CreateService(ctx context.Context, svc *corev1.Service)
	UpdatePod(ctx context.Context, pod *corev1.Pod)
}

func drainN(c chan string, n int) {
	for n > 0 {
		n--
		<-c
	}
}

var nextIP = net.ParseIP("10.0.0.10")

func GetIP() string {
	i := nextIP.To4()
	ret := i.String()
	v := uint(i[0])<<24 + uint(i[1])<<16 + uint(i[2])<<8 + uint(i[3])
	v++
	v3 := byte(v & 0xFF)
	v2 := byte((v >> 8) & 0xFF)
	v1 := byte((v >> 16) & 0xFF)
	v0 := byte((v >> 24) & 0xFF)
	nextIP = net.IPv4(v0, v1, v2, v3)
	return ret
}
