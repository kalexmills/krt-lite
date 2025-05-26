package testutils

import (
	"fmt"
	"net"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	krtlite "github.com/kalexmills/krt-lite"
	"github.com/stretchr/testify/assert"
)

const (
	Timeout      = time.Second * 5       // Timeout is used for all Eventually and context.WithTimeout calls.
	PollInterval = 20 * time.Millisecond // PollInterval interval is used for all Eventually
)

func ListSorted[T any](c krtlite.Collection[T]) []T {
	result := c.List()
	slices.SortFunc(result, func(a, b T) int {
		return strings.Compare(krtlite.GetKey(a), krtlite.GetKey(b))
	})
	return result
}

type Tracker[T any] struct {
	t *testing.T

	mut    *sync.RWMutex
	events map[string]struct{}
}

func NewTracker[T any](t *testing.T) Tracker[T] {
	return Tracker[T]{
		t:      t,
		mut:    &sync.RWMutex{},
		events: make(map[string]struct{}),
	}
}

func (t *Tracker[T]) Track(e krtlite.Event[T]) {
	t.mut.Lock()
	defer t.mut.Unlock()
	key := fmt.Sprintf("%v/%s", e.Type, krtlite.GetKey(e.Latest()))
	t.events[key] = struct{}{}
}

func (t *Tracker[T]) Empty() {
	t.t.Helper()
	t.mut.RLock()
	defer t.mut.RUnlock()
	assert.Empty(t.t, t.events)
}

// Wait waits for events to have occurred, no order is asserted.
func (t *Tracker[T]) Wait(events ...string) {
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
	}, Timeout, PollInterval, "expected events: %v", events)
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
	}, Timeout, PollInterval)
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
	}, Timeout, PollInterval)
	if !passed {
		t.Errorf("got: %v; want: %v", coll.List(), expectedObjs)
	}
}

func AssertEventually(t *testing.T, f func() bool, msgAndArgs ...any) {
	t.Helper()
	assert.Eventually(t, f, Timeout, PollInterval, msgAndArgs...)
}

func AssertEventuallyEqual(t *testing.T, expected any, getActual func() any, msgAndArgs ...any) {
	t.Helper()
	passed := assert.Eventually(t, func() bool {
		actual := getActual()
		return reflect.DeepEqual(expected, actual)
	}, Timeout, PollInterval, msgAndArgs...)

	if !passed {
		t.Errorf("want: %v; got: %v", expected, getActual())
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
