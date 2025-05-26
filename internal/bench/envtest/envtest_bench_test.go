package envtest_test

import (
	"context"
	"fmt"
	krtlite "github.com/kalexmills/krt-lite"
	"istio.io/istio/pkg/kube"
	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/kube/kubetypes"
	"istio.io/istio/pkg/slices"
	"istio.io/istio/pkg/test"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/rest"
	"log/slog"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"testing"

	. "github.com/kalexmills/krt-lite/internal/testutils"
)

// BenchmarkControllerEnvtest benchmarks krt against krt-lite against a local kube-apiserver process, backed by etcd.
//
// This aim of this benchmark is to take into account differences between the client supported by each library, by
// running against a real apiserver
func BenchmarkControllerEnvtest(b *testing.B) {
	ctx := b.Context()

	oldLevel := slog.SetLogLoggerLevel(slog.LevelInfo)
	defer slog.SetLogLoggerLevel(oldLevel)

	benchmark := func(b *testing.B, fn func(b *testing.B, cfg *rest.Config, events chan string) (Client, func())) {
		const K = 100

		// setup envtest and create namespaces for benchmark
		b.Logf("starting envtest environment")
		env, cfg, err := NewEnvtest()
		if err != nil {
			b.Fatal(fmt.Errorf("could not start envtest: %w", err))
		}
		defer func() {
			if err := env.Stop(); err != nil {
				b.Logf("could not stop envtest environment: %v", err)
			}
		}()

		nsClient, err := client.New(cfg, client.Options{})
		if err != nil {
			b.Fatal(fmt.Errorf("could not create client: %w", err))
		}

		b.Logf("creating namespaces")
		for _, ns := range []string{"ns-0", "ns-1"} {
			if err := nsClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
				b.Fatal(fmt.Errorf("could not create namespace %q: %w", ns, err))
			}
		}

		var initialPods []*corev1.ConfigMap
		for i := 0; i < K; i++ {
			initialPods = append(initialPods, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("pod-%d", i),
					Namespace: fmt.Sprintf("ns-%d", i%2),
					Labels: map[string]string{
						"app": fmt.Sprintf("app-%d", i%25),
						"pod": "true",
					},
				},
				Data: map[string]string{
					"podIP": GetIP(),
				},
			})
		}

		var initialServices []*corev1.ConfigMap
		for i := 0; i < 50; i++ {
			initialServices = append(initialServices, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("svc-%d", i),
					Namespace: fmt.Sprintf("ns-%d", i%2),
				},
				Data: map[string]string{ // representing a match label selector.
					"app": fmt.Sprintf("app-%d", i%25),
					"svc": "true",
				},
			})
		}

		ctx := b.Context()
		events := make(chan string, K)

		b.Logf("starting controller")
		c, wait := fn(b, cfg, events)

		b.Logf("creating initial services and pods")
		for _, pod := range initialPods {
			c.CreatePod(ctx, pod)
		}
		for _, s := range initialServices {
			c.CreateService(ctx, s)
		}

		b.Logf("waiting for sync")
		wait()
		b.Logf("starting benchmark")

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			for i := 0; i < K; i++ {
				c.UpdatePod(ctx, &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("pod-%d", i),
						Namespace: fmt.Sprintf("ns-%d", i%2),
						Labels: map[string]string{
							"app": fmt.Sprintf("app-%d", i%25),
							"pod": "true",
						},
					},
					Data: map[string]string{
						"podIP": GetIP(),
					},
				})
			}
			drainN(events, K)
		}
	}

	b.Run("krt", func(b *testing.B) {
		benchmark(b, KrtController)
		b.Log("finished with krt benchmark")
	})

	b.Run("krt-lite", func(b *testing.B) {
		benchmark(b, KrtLiteController)
		b.Log("finished with krt-lite benchmark")
	})
}

func NewEnvtest() (*envtest.Environment, *rest.Config, error) {
	env := &envtest.Environment{}

	cfg, err := env.Start()
	return env, cfg, err
}

type krtClientWrapper struct {
	client kube.Client
}

func (c *krtClientWrapper) CreatePod(ctx context.Context, pod *corev1.ConfigMap) {
	_, err := c.client.Kube().CoreV1().ConfigMaps(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		panic(fmt.Errorf("error updating pod: %w", err))
	}
}

func (c *krtClientWrapper) CreateService(ctx context.Context, svc *corev1.ConfigMap) {
	_, err := c.client.Kube().CoreV1().ConfigMaps(svc.Namespace).Create(ctx, svc, metav1.CreateOptions{})
	if err != nil {
		panic(fmt.Errorf("error updating pod: %w", err))
	}
}

func (c *krtClientWrapper) UpdatePod(ctx context.Context, pod *corev1.ConfigMap) {
	_, err := c.client.Kube().CoreV1().ConfigMaps(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
	if err != nil {
		panic(fmt.Errorf("error updating pod: %w", err))
	}
}

type ServiceWrapper struct{ *corev1.ConfigMap }

func (s ServiceWrapper) GetLabelSelector() map[string]string {
	return s.Data
}

func KrtController(b *testing.B, cfg *rest.Config, events chan string) (Client, func()) {
	clientConfig := kube.NewClientConfigForRestConfig(cfg)
	c, err := kube.NewClient(clientConfig, "envtest")
	if err != nil {
		b.Fatal(fmt.Errorf("could not create client: %w", err))
	}

	Pods := krt.NewInformerFiltered[*corev1.ConfigMap](c, kubetypes.Filter{LabelSelector: "pod=true"})
	Services := krt.NewInformerFiltered[*corev1.ConfigMap](c, kubetypes.Filter{LabelSelector: "svc=true"},
		krt.WithObjectAugmentation(func(o any) any {
			return ServiceWrapper{o.(*corev1.ConfigMap)}
		}),
	)
	ServicesByNamespace := krt.NewNamespaceIndex(Services)

	Workloads := krt.NewCollection(Pods, func(ctx krt.HandlerContext, p *corev1.ConfigMap) *Workload {
		podIP, ok := p.Data["podIP"]
		if !ok || podIP == "" {
			return nil
		}
		services := krt.Fetch(ctx, Services, krt.FilterIndex(ServicesByNamespace, p.Namespace), krt.FilterSelectsNonEmpty(p.GetLabels()))
		return &Workload{
			Named:        NewNamed(p),
			IP:           podIP,
			ServiceNames: slices.Map(services, func(e *corev1.ConfigMap) string { return e.Name }),
		}
	})
	Workloads.Register(func(e krt.Event[Workload]) {
		events <- fmt.Sprintf(e.Latest().Name, e.Event)
	})
	wrapped := &krtClientWrapper{
		client: c,
	}
	waitForSync := func() {
		c.RunAndWait(test.NewStop(b))
	}
	return wrapped, waitForSync
}

type krtliteWrapper struct {
	client client.WithWatch
}

func (k *krtliteWrapper) CreatePod(ctx context.Context, pod *corev1.ConfigMap) {
	if err := k.client.Create(ctx, pod); err != nil {
		panic(fmt.Errorf("error creating pod: %w", err))
	}
}

func (k *krtliteWrapper) CreateService(ctx context.Context, svc *corev1.ConfigMap) {
	if err := k.client.Create(ctx, svc); err != nil {
		panic(fmt.Errorf("error creating service: %w", err))
	}
}

func (k *krtliteWrapper) UpdatePod(ctx context.Context, pod *corev1.ConfigMap) {
	if err := k.client.Update(ctx, pod); err != nil {
		panic(fmt.Errorf("error updating pod: %w", err))
	}
}

func KrtLiteController(b *testing.B, cfg *rest.Config, events chan string) (Client, func()) {
	ctx := b.Context()

	k8sClient, err := client.NewWithWatch(cfg, client.Options{})
	if err != nil {
		b.Fatal(fmt.Errorf("could not create client: %w", err))
	}

	Pods := krtlite.NewInformer[*corev1.ConfigMap, corev1.ConfigMapList](ctx, k8sClient,
		krtlite.WithFilterByLabel("pod=true"),
		krtlite.WithName("Pods"), krtlite.WithContext(ctx))
	Services := krtlite.NewInformer[*corev1.ConfigMap, corev1.ConfigMapList](ctx, k8sClient,
		krtlite.WithFilterByLabel("svc=true"),
		krtlite.WithName("Services"), krtlite.WithContext(ctx))
	ServicesByNamespace := krtlite.NewNamespaceIndex(Services)

	Workloads := krtlite.Map(Pods, func(ktx krtlite.Context, p *corev1.ConfigMap) *Workload {
		podIP, ok := p.Data["podIP"]
		if !ok || podIP == "" {
			return nil
		}

		services := krtlite.Fetch(ktx, Services, krtlite.MatchIndex(ServicesByNamespace, p.Namespace),
			krtlite.MatchSelectsLabels(p.Labels, func(obj any) labels.Set {
				svc := obj.(*corev1.ConfigMap)
				return svc.Data
			}))
		result := &Workload{
			Named: NewNamed(p),
			IP:    podIP,
		}

		for _, service := range services {
			if labels.Set(service.Labels).AsSelector().Matches(labels.Set(p.Labels)) {
				result.ServiceNames = append(result.ServiceNames, service.Name)
			}
		}
		return result
	}, krtlite.WithName("Workloads"), krtlite.WithContext(ctx))

	reg := Workloads.Register(func(e krtlite.Event[Workload]) {
		events <- fmt.Sprintf("%s-%s", e.Latest().Name, e.Type)
	})

	return &krtliteWrapper{client: k8sClient}, func() {
		reg.WaitUntilSynced(ctx.Done())
	}
}

type Client interface {
	CreatePod(ctx context.Context, pod *corev1.ConfigMap)
	CreateService(ctx context.Context, svc *corev1.ConfigMap)
	UpdatePod(ctx context.Context, pod *corev1.ConfigMap)
}

func drainN(c chan string, n int) {
	for n > 0 {
		n--
		<-c
	}
}
