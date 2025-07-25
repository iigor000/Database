package memtable

import "github.com/iigor000/database/structures/adapter"

type MemtableIterator struct {
	memtable     *Memtable
	currentEntry adapter.MemtableEntry
}

func (m *Memtable) NewMemtableIterator() *MemtableIterator {
	if m.Size == 0 {
		return nil // Nema unosa u Memtable
	}
	println("Creating MemtableIterator for Memtable with size:", m.Size)
	return &MemtableIterator{
		memtable:     m,
		currentEntry: m.GetFirstEntry(),
	}
}
func (mi *MemtableIterator) Next() (adapter.MemtableEntry, bool) {
	if mi.currentEntry.Key == nil {
		return adapter.MemtableEntry{}, false // Nema više unosa
	}
	rec := mi.currentEntry
	nextEntry, found := mi.memtable.GetNextEntry(mi.currentEntry.Key)
	if !found {
		mi.Stop() // Zatvaranje iteratora ako nema više unosa
	}
	mi.currentEntry = nextEntry
	return rec, true
}
func (mi *MemtableIterator) Stop() {
	mi.memtable = nil
	mi.currentEntry = adapter.MemtableEntry{Key: nil}
}

type MemtablesIterator struct {
	memtables *Memtables
	iterators []*MemtableIterator
}

func (ms *Memtables) NewMemtablesIterator() *MemtablesIterator {
	if len(ms.Memtables) == 0 {
		return nil // Nema unosa u Memtables
	}
	println("Creating MemtablesIterator for Memtables with size:", len(ms.Memtables))
	iterators := make([]*MemtableIterator, len(ms.Memtables))
	for i, memtable := range ms.Memtables {
		iterators[i] = memtable.NewMemtableIterator()
	}
	return &MemtablesIterator{
		memtables: ms,
		iterators: iterators,
	}
}
