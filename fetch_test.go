package krtlite_test

import (
	krtlite "github.com/kalexmills/krt-lite"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"testing"
)

func TestFetchOptions(t *testing.T) {
	ctx := t.Context()

	keepLabels := map[string]string{"keep": "true"}
	rejectLabels := map[string]string{"keep": "false"}
	keep := func(ns, name string) LabeledNamed {
		return LabeledNamed{
			Named:   Named{Name: name, Namespace: ns},
			Labeled: Labeled{Labels: keepLabels},
		}
	}
	reject := func(ns, name string) LabeledNamed {
		return LabeledNamed{
			Named:   Named{Name: name, Namespace: ns},
			Labeled: Labeled{Labels: rejectLabels},
		}
	}

	Source := krtlite.NewSingleton[Named](&Named{}, true, krtlite.WithName("Source"))

	simpleTest := func(fetchOption krtlite.FetchOption) func(t *testing.T) {
		return func(t *testing.T) {
			Names := krtlite.NewStaticCollection[LabeledNamed](nil, []LabeledNamed{
				keep("ns", "a"),
				reject("ns", "b"),
				reject("ns", "c"),
				keep("ns", "d"),
				reject("ns", "e"),
			}, krtlite.WithName("Names"))

			Filtered := krtlite.FlatMap(Source, func(ktx krtlite.Context, n Named) []LabeledNamed {
				return krtlite.Fetch(ktx, Names, fetchOption)
			}, krtlite.WithName("Filtered"), krtlite.WithStop(t.Context().Done()))

			Filtered.WaitUntilSynced(ctx.Done())
			AssertEventuallyKeysMatch(t, Filtered, "ns/a", "ns/d")
		}
	}

	t.Run("MatchKeys", simpleTest(krtlite.MatchKeys("ns/a", "ns/d")))
	t.Run("MatchLabels", simpleTest(krtlite.MatchLabels(keepLabels)))
	t.Run("MatchLabelSelector", simpleTest(krtlite.MatchLabelSelector(labels.SelectorFromSet(keepLabels))))
	t.Run("MatchFilter", simpleTest(krtlite.MatchFilter(func(t LabeledNamed) bool {
		return t.Labels["keep"] == "true"
	})))

	service := func(ns, name string, keep bool) *corev1.Service {
		result := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		}

		if keep {
			result.Spec.Selector = keepLabels
		} else {
			result.Spec.Selector = rejectLabels
		}
		return result
	}

	serviceTest := func(fetchOption krtlite.FetchOption) func(t *testing.T) {
		return func(t *testing.T) {
			Services := krtlite.NewStaticCollection[*corev1.Service](nil,
				[]*corev1.Service{
					service("ns", "a", true),
					service("ns", "b", false),
					service("ns", "c", false),
					service("ns", "d", true),
					service("ns", "e", false),
				},
			)
			Filtered := krtlite.FlatMap(Source, func(ktx krtlite.Context, n Named) []*corev1.Service {
				return krtlite.Fetch(ktx, Services, fetchOption)
			})

			Filtered.WaitUntilSynced(ctx.Done())
			AssertEventuallyKeysMatch(t, Filtered, "ns/a", "ns/d")
		}
	}
	t.Run("MatchSelectsLabels", serviceTest(krtlite.MatchSelectsLabels(keepLabels, krtlite.ExtractPodSelector)))
	t.Run("MatchNames", serviceTest(krtlite.MatchNames("ns/a", "ns/d")))
}
