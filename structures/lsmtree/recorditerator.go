package lsmtree

import (
	"bytes"

	"github.com/iigor000/database/structures/sstable"
)

// RecordIterator služi za iteraciju kroz zapise SSTable-a
type RecordIterator struct {
	records []sstable.DataRecord
	index   int
}

func NewRecordIterator(records []sstable.DataRecord) *RecordIterator {
	return &RecordIterator{records: records, index: 0}
}

func (it *RecordIterator) HasNext() bool {
	return it.index < len(it.records)
}

// Peek vraća trenutni zapis bez pomeranja iteratora
func (it *RecordIterator) Peek() *sstable.DataRecord {
	if it.HasNext() {
		return &it.records[it.index]
	}
	return nil
}

// Next vraća sledeći zapis i pomera iterator
func (it *RecordIterator) Next() *sstable.DataRecord {
	if it.HasNext() {
		rec := &it.records[it.index]
		it.index++
		return rec
	}
	return nil
}

// HeapItem je element MinHeap-a koji sadrži DataRecord i RecordIterator
// Koristi se za sortiranje i pristupanje najmanjem ključu (pri Merge operacijama)
type HeapItem struct {
	record   *sstable.DataRecord
	iterator *RecordIterator
}

type MinHeap []*HeapItem

func (h MinHeap) Len() int { return len(h) }

func (h MinHeap) Less(i, j int) bool {
	return bytes.Compare(h[i].record.Key, h[j].record.Key) < 0
}

func (h MinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// Push i Pop su metode koje implementiraju heap interfejs
func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(*HeapItem))
}

func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}
