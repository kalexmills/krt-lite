/*
The MIT License (MIT)

Copyright (c) 2014 Evan Huus

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

// NOTICE: file copied from github.com/eapache/queue/v2 -- modifications have been made.

/*
Package fifo provides a fast, ring-buffer queue based on the version suggested by Dariusz Górecki.
Using this instead of other, simpler, queue implementations (slice+append or linked list) provides
substantial memory and time benefits, and fewer GC pauses.

The queue implemented here is as fast as it is for an additional reason: it is *not* thread-safe.
*/

package fifo

const minRingbufLen = 32

// ringbuf represents a single instance of the ringbuf data structure.
type ringbuf[V any] struct {
	buf               []*V
	head, tail, count int
}

// newRingBuf constructs and returns a new ringbuf.
func newRingBuf[V any](size int) *ringbuf[V] {
	return &ringbuf[V]{
		buf: make([]*V, max(minRingbufLen, size)),
	}
}

// Reset resets this ring buffer without reallocating any new memory.
func (q *ringbuf[V]) Reset() {
	q.head = 0
	q.tail = 0
	q.count = 0
}

// Len returns the number of elements currently stored in the ringbuf.
func (q *ringbuf[V]) Len() int {
	return q.count
}

func (q *ringbuf[V]) Cap() int {
	return cap(q.buf)
}

// resizes the ringbuf to fit exactly twice its current contents.
// this can result in shrinking if the ringbuf is less than half-full.
func (q *ringbuf[V]) resize() {
	newBuf := make([]*V, q.count<<1)

	if q.tail > q.head {
		copy(newBuf, q.buf[q.head:q.tail])
	} else {
		n := copy(newBuf, q.buf[q.head:])
		copy(newBuf[n:], q.buf[:q.tail])
	}

	q.head = 0
	q.tail = q.count
	q.buf = newBuf
}

// Add puts an element on the end of the ringbuf.
func (q *ringbuf[V]) Add(elem V) {
	if q.count == len(q.buf) {
		q.resize()
	}

	q.buf[q.tail] = &elem
	// bitwise modulus
	q.tail = (q.tail + 1) & (len(q.buf) - 1)
	q.count++
}

// Peek returns the element at the head of the ringbuf. This call panics
// if the ringbuf is empty.
func (q *ringbuf[V]) Peek() V {
	if q.count <= 0 {
		panic("ringbuf: Peek() called on empty ringbuf")
	}
	return *(q.buf[q.head])
}

// Get returns the element at index i in the ringbuf. If the index is
// invalid, the call will panic. This method accepts both positive and
// negative index values. Index 0 refers to the first element, and
// index -1 refers to the last.
func (q *ringbuf[V]) Get(i int) V {
	// If indexing backwards, convert to positive index.
	if i < 0 {
		i += q.count
	}
	if i < 0 || i >= q.count {
		panic("ringbuf: Get() called with index ready of range")
	}
	// bitwise modulus
	return *(q.buf[(q.head+i)&(len(q.buf)-1)])
}

// Remove removes and returns the element from the front of the ringbuf. If the
// ringbuf is empty, the call will panic.
func (q *ringbuf[V]) Remove() V {
	if q.count <= 0 {
		panic("ringbuf: Remove() called on empty ringbuf")
	}
	ret := q.buf[q.head]
	q.buf[q.head] = nil
	// bitwise modulus
	q.head = (q.head + 1) & (len(q.buf) - 1)
	q.count--
	// Resize down if buffer 1/4 full.
	if len(q.buf) > minRingbufLen && (q.count<<2) == len(q.buf) {
		q.resize()
	}
	return *ret
}
