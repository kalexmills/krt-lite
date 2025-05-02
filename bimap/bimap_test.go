package bimap_test

import (
	"github.com/kalexmills/krt-lite/bimap"
	"github.com/stretchr/testify/assert"
	"slices"
	"testing"
)

func TestBimap(t *testing.T) {
	bimap := bimap.New[int, int]()

	zeroToNine := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	for j := 0; j < 10; j++ {
		for i := 0; i < 10; i++ {
			bimap.Add(i, j)
		}

		var oneToJ []int
		for k := 0; k < j+1; k++ {
			oneToJ = append(oneToJ, k)
		}
		assert.Equal(t, zeroToNine, slices.Sorted(bimap.GetUs(j)))
		for _, i := range zeroToNine {
			assert.Equal(t, oneToJ, slices.Sorted(bimap.GetVs(i)))
		}
	}

	for j := 0; j < 10; j++ {
		bimap.RemoveLeft(j)

		for i := j + 1; i < 10; i++ {
			assert.Equal(t, zeroToNine, slices.Sorted(bimap.GetVs(i)))
		}
		var jPlusOneToNine []int
		for k := j + 1; k < 10; k++ {
			jPlusOneToNine = append(jPlusOneToNine, k)
		}
		for i := 0; i < 10; i++ {
			assert.Equal(t, jPlusOneToNine, slices.Sorted(bimap.GetUs(i)))
		}
	}
	assert.True(t, bimap.IsEmpty())
}
