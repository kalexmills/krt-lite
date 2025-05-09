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

	// add and assert
	for j := 0; j < 10; j++ {
		for i := 0; i < 10; i++ {
			bimap.Add(i, j)
			assert.Equal(t, 10*j+(i+1), bimap.Size())
		}

		var oneToJ []int
		for k := 0; k < j+1; k++ {
			oneToJ = append(oneToJ, k)
		}
		assert.Equal(t, zeroToNine, slices.Sorted(bimap.GetLeft(j)))
		for _, i := range zeroToNine {
			assert.Equal(t, oneToJ, slices.Sorted(bimap.GetRight(i)))
		}
	}

	assert.Equal(t, 100, bimap.Size())

	// remove from left set
	for j := 0; j < 10; j++ {
		bimap.RemoveLeft(j)
		assert.Equal(t, 100-10*(j+1), bimap.Size())

		for i := j + 1; i < 10; i++ {
			assert.Equal(t, zeroToNine, slices.Sorted(bimap.GetRight(i)))
		}
		var jPlusOneToNine []int
		for k := j + 1; k < 10; k++ {
			jPlusOneToNine = append(jPlusOneToNine, k)
		}
		for i := 0; i < 10; i++ {
			assert.Equal(t, jPlusOneToNine, slices.Sorted(bimap.GetLeft(i)))
		}
	}
	assert.True(t, bimap.IsEmpty())
	assert.Equal(t, 0, bimap.Size())

	// recreate and remove from right set
	for j := 0; j < 10; j++ {
		for i := 0; i < 10; i++ {
			bimap.Add(i, j)
		}
	}

	assert.Equal(t, 100, bimap.Size())

	// remove from right set
	for j := 0; j < 10; j++ {
		bimap.RemoveRight(j)
		assert.Equal(t, 100-10*(j+1), bimap.Size())

		for i := j + 1; i < 10; i++ {
			assert.Equal(t, zeroToNine, slices.Sorted(bimap.GetLeft(i)))
		}
		var jPlusOneToNine []int
		for k := j + 1; k < 10; k++ {
			jPlusOneToNine = append(jPlusOneToNine, k)
		}
		for i := 0; i < 10; i++ {
			assert.Equal(t, jPlusOneToNine, slices.Sorted(bimap.GetRight(i)))
		}
	}
	assert.True(t, bimap.IsEmpty())
	assert.Equal(t, 0, bimap.Size())
}
