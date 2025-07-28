package lsmtree

import (
	"bytes"
	"container/heap"

	"github.com/iigor000/database/structures/adapter"
)

// MergedIterator kombinuje više GenericIterator-a u jedan
// Omogućava iteraciju kroz sve elemente iz svih iteratora, sortirane po ključu i vremenu
type MergedIterator struct {
	minHeap *MinHeap
}

func NewMergedIterator(iters ...GenericIterator) *MergedIterator {
	var h MinHeap

	for _, iter := range iters {
		if iter.HasNext() {
			entry, err := iter.Next()
			if err != nil {
				continue
			}
			heap.Push(&h, &iteratorItem{
				iterator: iter,
				current:  *entry,
			})
		}
	}

	heap.Init(&h)

	return &MergedIterator{
		minHeap: &h,
	}
}

func (m *MergedIterator) HasNext() bool {
	return m.minHeap.Len() > 0
}

// Next vraća sledeći MemtableEntry iz MergedIterator-a
func (m *MergedIterator) Next() *adapter.MemtableEntry {
	if !m.HasNext() {
		return nil
	}

	// Izvuci najmanji key
	item := heap.Pop(m.minHeap).(*iteratorItem)
	current := item.current

	// Učitaj sledeći iz istog iteratora ako ima
	if item.iterator.HasNext() {
		next, err := item.iterator.Next()
		if err == nil {
			heap.Push(m.minHeap, &iteratorItem{
				current:  *next,
				iterator: item.iterator,
			})
		}
	}

	// Prati najnoviji entry za ovaj key
	latest := current

	// Preskoči sve duplikate sa istim ključem, ali manjim timestampom
	for m.HasNext() {
		next := m.minHeap.Peek()
		if next == nil {
			break
		}

		// Ako sledeći ključ nije isti, izađi
		if !bytes.Equal(next.current.Key, current.Key) {
			break
		}

		// Ako je isti, izvuci ga
		next = heap.Pop(m.minHeap).(*iteratorItem)

		// Odaberi onaj sa većim timestampom (noviji)
		if next.current.Timestamp > latest.Timestamp {
			latest = next.current
		}

		// Ako taj iterator ima sledeći element, dodaj ga
		if next.iterator.HasNext() {
			nextEntry, err := next.iterator.Next()
			if err != nil {
				return nil
			}
			heap.Push(m.minHeap, &iteratorItem{
				current:  *nextEntry,
				iterator: next.iterator,
			})
		}
	}

	return &latest
}

// Close zatvara sve iterator-e u MergedIterator-u
func (m *MergedIterator) Close() {
	seen := map[GenericIterator]bool{}

	for m.minHeap.Len() > 0 {
		item := heap.Pop(m.minHeap).(*iteratorItem)
		if !seen[item.iterator] {
			item.iterator.Close()
			seen[item.iterator] = true
		}
	}
}
