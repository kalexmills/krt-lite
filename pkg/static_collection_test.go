package pkg_test

import (
	"context"
	krtlite "github.com/kalexmills/krt-lite/pkg"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStaticCollection(t *testing.T) {
	_, cancel := context.WithCancel(t.Context())
	defer cancel()

	c := krtlite.NewStaticCollection[Named](nil, []Named{{"ns", "a"}}, krtlite.WithName("c"))
	assert.True(t, c.HasSynced(), "should start synced")
	assert.Equal(t, []Named{{"ns", "a"}}, c.List())

	tr := NewTracker[Named](t)
	c.Register(tr.Track)
	tr.Wait("add/ns/a")

	c.Update(Named{"ns", "b"})
	tr.Wait("add/ns/b")

	c.Update(Named{"ns", "b"})
	tr.Wait("update/ns/b")

	tr2 := NewTracker[Named](t)
	c.Register(tr2.Track)
	tr2.Wait("add/ns/a", "add/ns/b")

	c.Delete("ns/b")
	tr.Wait("delete/ns/b")
}
