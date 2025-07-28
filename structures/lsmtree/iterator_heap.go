package lsmtree

import (
	"bytes"

	"github.com/iigor000/database/structures/adapter"
)

// iteratorItem predstavlja jedan element u MinHeap-u
type iteratorItem struct {
	iterator GenericIterator
	current  adapter.MemtableEntry
}
type MinHeap []*iteratorItem

func (h MinHeap) Len() int      { return len(h) }
func (h MinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
// Less poredi dva elementa u MinHeap-u
// Prvo po Key-u (leksikografski), ako su isti, onda po Timestamp-u (veći timestamp je "noviji")
// Ovo omogućava da se u MinHeap-u uvek nalazi najažurniji MemtableEntry
func (h MinHeap) Less(i, j int) bool {
	// Prvo po Key-u (leksikografski)
	cmp := bytes.Compare(h[i].current.Key, h[j].current.Key)
	if cmp != 0 {
		return cmp < 0
	}
	// Ako je isti key, onda po timestampu (veći timestamp je "noviji")
	return h[i].current.Timestamp > h[j].current.Timestamp
}
func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(*iteratorItem))
}

func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

func (h MinHeap) Peek() *iteratorItem {
	if len(h) == 0 {
		return nil
	}
	return h[0]
}
