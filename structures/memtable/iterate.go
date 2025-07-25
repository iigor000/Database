package memtable

import (
	"bytes"

	"github.com/iigor000/database/structures/adapter"
)

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
	memtables    *Memtables
	iterators    []*MemtableIterator
	currentEntry adapter.MemtableEntry
}

func (ms *Memtables) NewMemtablesIterator() *MemtablesIterator {
	if len(ms.Memtables) == 0 {
		return nil // Nema unosa u Memtables
	}
	currentEntry := ms.GetFirstEntry()
	println("Creating MemtablesIterator for Memtables with size:", len(ms.Memtables))
	iterators := make([]*MemtableIterator, len(ms.Memtables))
	for _, memtable := range ms.Memtables {
		it := memtable.NewMemtableIterator()
		// Dodaj null check
		if it != nil {
			it.currentEntry = currentEntry // Postavi trenutni unos na prvi unos
			iterators = append(iterators, it)
		}
	}

	// Ako nema validnih iteratora, vrati nil
	if len(iterators) == 0 {
		return nil
	}
	return &MemtablesIterator{
		memtables:    ms,
		iterators:    iterators,
		currentEntry: currentEntry,
	}
}

func (mi *MemtablesIterator) Next() (adapter.MemtableEntry, bool) {
	if mi.currentEntry.Key == nil {
		return adapter.MemtableEntry{}, false // Nema više unosa
	}
	rec := mi.currentEntry
	newEntry := mi.currentEntry
	minKey := mi.currentEntry.Key
	iIndex := -1
	for i, iterator := range mi.iterators {
		if iterator == nil {
			continue
		}
		key, found := iterator.Next()

		if !found {
			continue // Nema više unosa u ovom iteratoru
		}
		if bytes.Equal(mi.currentEntry.Key, minKey) {
			minKey = key.Key
			iIndex = i
			newEntry = key
		}
		if bytes.Compare(iterator.currentEntry.Key, minKey) < 0 {
			minKey = key.Key
			iIndex = i
			newEntry = key
		}

	}
	if iIndex == -1 {
		mi.Stop() // Zatvaranje iteratora ako nema vise unosa
	}
	mi.currentEntry = newEntry
	for _, iterator := range mi.iterators {
		if iterator == nil || iterator.currentEntry.Key == nil {
			continue
		}
		iterator.currentEntry = newEntry
	}
	return rec, true
}

func (mi *MemtablesIterator) Stop() {
	for _, iterator := range mi.iterators {
		iterator.Stop()
	}
	mi.memtables = nil
	mi.iterators = nil
	mi.currentEntry = adapter.MemtableEntry{Key: nil}
}
