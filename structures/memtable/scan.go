package memtable

import "github.com/iigor000/database/structures/adapter"

func (m *Memtables) RangeScan(startKey, endKey []byte, pageNumber int, pageSize int) []adapter.MemtableEntry {
	rangeIterator := m.RangeIterate(startKey, endKey)
	entries := make([]adapter.MemtableEntry, 0)
	startIndex := (pageNumber - 1) * pageSize
	endIndex := startIndex + pageSize
	if startIndex < 0 {
		startIndex = 0
	}
	for i := 0; i < startIndex; i++ {
		if _, ok := rangeIterator.Next(); !ok {

			return entries
		}
	}
	for i := startIndex; i < endIndex; i++ {
		entry, ok := rangeIterator.Next()
		if !ok {
			break
		}
		entries = append(entries, entry)
	}
	return entries
}

func (m *Memtables) PrefixScan(prefix string, pageNumber int, pageSize int) []adapter.MemtableEntry {
	prefixIterator := m.PrefixIterate(prefix)
	entries := make([]adapter.MemtableEntry, 0)
	startIndex := (pageNumber - 1) * pageSize
	endIndex := startIndex + pageSize
	if startIndex < 0 {
		startIndex = 0
	}
	for i := 0; i < startIndex; i++ {
		if _, ok := prefixIterator.Next(); !ok {
			return entries
		}
	}
	for i := startIndex; i < endIndex; i++ {
		entry, ok := prefixIterator.Next()
		if !ok {
			break
		}
		entries = append(entries, entry)
	}
	return entries
}
