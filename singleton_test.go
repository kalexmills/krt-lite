package krt_lite_test

import (
	"testing"

	krtlite "github.com/kalexmills/krt-lite"
	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"
)

func TestStaticSingleton(t *testing.T) {
	tt := NewTracker[string](t)
	s := krtlite.NewSingleton[string](nil, true)
	s.Register(tt.Track)

	assert.Equal(t, (*string)(nil), s.Get())

	s.Set(ptr.To("foo"))
	assert.Equal(t, ptr.To("foo"), s.Get())
	tt.Wait("add/foo")

	s.Set(nil)
	assert.Equal(t, (*string)(nil), s.Get())
	tt.Wait("delete/foo")

	s.Set(ptr.To("bar"))
	assert.Equal(t, ptr.To("bar"), s.Get())
	tt.Wait("add/bar")

	s.Set(ptr.To("bar2"))
	assert.Equal(t, ptr.To("bar2"), s.Get())
	tt.Wait("update/bar2")
}
