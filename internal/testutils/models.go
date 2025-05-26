package testutils

import (
	"context"
	krtlite "github.com/kalexmills/krt-lite"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

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

type Workload struct {
	Named
	ServiceNames []string
	IP           string
}

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
