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

type MemtablePrefixIterator struct {
	memtableIterator *MemtableIterator
	prefix           string
}

func (m *Memtable) PrefixIterate(prefix string) *MemtablePrefixIterator {
	if m.Size == 0 {
		return nil // Nema unosa u Memtable
	}
	it := m.NewMemtableIterator()
	if it == nil {
		return nil // Nema unosa u Memtable
	}
	entry, ok := m.GetFirstEntryWithPrefix(prefix)
	if !ok {
		return nil // Nema unosa sa datim prefiksom
	}
	it.currentEntry = entry
	return &MemtablePrefixIterator{
		memtableIterator: it,
		prefix:           prefix,
	}
}
func (mpi *MemtablePrefixIterator) Next() (adapter.MemtableEntry, bool) {
	if mpi.memtableIterator.currentEntry.Key == nil {
		return adapter.MemtableEntry{}, false
	}
	rec := mpi.memtableIterator.currentEntry
	nextEntry, found := mpi.memtableIterator.memtable.GetNextEntryWithPrefix(mpi.memtableIterator.currentEntry.Key, mpi.prefix)
	if !found {
		mpi.Stop()
	}
	mpi.memtableIterator.currentEntry = nextEntry
	return rec, true
}
func (mpi *MemtablePrefixIterator) Stop() {
	mpi.memtableIterator.Stop()
	mpi.prefix = ""
}

type MemtableRangeIterator struct {
	memtableIterator *MemtableIterator
	startKey         []byte
	endKey           []byte
}

func (m *Memtable) RangeIterate(startKey, endKey []byte) *MemtableRangeIterator {
	if m.Size == 0 {
		return nil
	}
	it := m.NewMemtableIterator()
	if it == nil {
		return nil
	}
	entry, ok := m.GetNextKey(startKey)
	if !ok {
		return nil // Nema unosa posle startKey
	}
	it.currentEntry = entry
	return &MemtableRangeIterator{
		memtableIterator: it,
		startKey:         startKey,
		endKey:           endKey,
	}
}

func (mri *MemtableRangeIterator) Next() (adapter.MemtableEntry, bool) {
	if mri.memtableIterator.currentEntry.Key == nil {
		return adapter.MemtableEntry{}, false
	}
	rec := mri.memtableIterator.currentEntry
	_, found := mri.memtableIterator.Next()
	if !found {
		mri.Stop()
	}
	nextEntry := mri.memtableIterator.currentEntry
	if nextEntry.Key == nil || bytes.Compare(nextEntry.Key, mri.endKey) > 0 {
		mri.Stop()
	}
	mri.memtableIterator.currentEntry = nextEntry
	return rec, true
}

func (mri *MemtableRangeIterator) Stop() {
	mri.memtableIterator.Stop()
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
		return adapter.MemtableEntry{}, false // Nema vise unosa
	}
	rec := mi.currentEntry
	newEntry := mi.currentEntry
	minKey := mi.currentEntry.Key
	iIndex := -1
	for i, iterator := range mi.iterators {
		if iterator == nil {
			continue
		}
		_, found := iterator.Next()
		key := iterator.currentEntry
		if key.Key == nil {
			continue // Nema vise unosa u ovom iteratoru
		}
		if !found {
			continue // Nema vise unosa u ovom iteratoru
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
		} else if bytes.Equal(key.Key, minKey) {
			if key.Timestamp > newEntry.Timestamp {
				minKey = key.Key
				iIndex = i
				newEntry = key
			}
		}
	}
	if iIndex == -1 {
		if mi != nil {
			mi.Stop()
		}
	}
	if bytes.Equal(rec.Key, newEntry.Key) {
		if mi != nil {
			mi.Stop()
		}
		return rec, true // Nema promene, vratimo trenutni unos
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
		if iterator != nil {
			iterator.Stop()
		}
	}
	mi.memtables = nil
	mi.iterators = nil
	mi.currentEntry = adapter.MemtableEntry{Key: nil}
}

type PrefixIterator struct {
	memtables    *Memtables
	iterators    []*MemtablePrefixIterator
	currentEntry adapter.MemtableEntry
}

func (ms *Memtables) PrefixIterate(prefix string) *PrefixIterator {
	if len(ms.Memtables) == 0 {
		return nil // Nema unosa u Memtables
	}
	iterators := make([]*MemtablePrefixIterator, 0)
	for _, memtable := range ms.Memtables {
		it := memtable.PrefixIterate(prefix)
		if it != nil {
			iterators = append(iterators, it)
		}
	}
	if len(iterators) == 0 {
		return nil
	}
	minKey := []byte{}
	currentEntry := adapter.MemtableEntry{Key: nil}
	for i := 0; i < len(iterators); i++ {
		if iterators[i] == nil {
			continue // Nema unosa u ovom iteratoru
		}
		key := iterators[i].memtableIterator.currentEntry
		if key.Key == nil {
			continue // Nema unosa u ovom iteratoru
		}

		if len(minKey) == 0 {
			minKey = key.Key
			currentEntry = key
		}
		if bytes.Compare(key.Key, minKey) < 0 {
			minKey = key.Key
			currentEntry = key
		}
		if bytes.Equal(key.Key, minKey) {
			if key.Timestamp > currentEntry.Timestamp {
				minKey = key.Key
				currentEntry = key
			}
		}

	}
	if len(minKey) == 0 {
		return nil
	}

	for i := 0; i < len(iterators); i++ {
		if iterators[i] != nil {
			iterators[i].memtableIterator.currentEntry = currentEntry
		}
	}
	return &PrefixIterator{
		memtables:    ms,
		iterators:    iterators,
		currentEntry: currentEntry,
	}
}

func (pi *PrefixIterator) Next() (adapter.MemtableEntry, bool) {
	if pi.currentEntry.Key == nil {
		return adapter.MemtableEntry{}, false
	}
	rec := pi.currentEntry
	newEntry := pi.currentEntry
	minKey := pi.currentEntry.Key
	iIndex := -1
	for i, iterator := range pi.iterators {

		if iterator == nil {
			continue
		}
		_, found := iterator.Next()
		key := iterator.memtableIterator.currentEntry
		if !found {
			continue
		}
		if key.Key == nil {
			continue
		}
		if bytes.Equal(pi.currentEntry.Key, minKey) {
			minKey = key.Key
			iIndex = i
			newEntry = key
		}
		if bytes.Compare(iterator.memtableIterator.currentEntry.Key, minKey) < 0 {
			minKey = key.Key
			iIndex = i
			newEntry = key
		} else if bytes.Equal(key.Key, minKey) {
			if key.Timestamp > newEntry.Timestamp {
				minKey = key.Key
				iIndex = i
				newEntry = key
			}
		}
	}
	if iIndex == -1 {
		pi.Stop() // Zatvaranje iteratora ako nema vise unosa
	}
	if bytes.Equal(rec.Key, newEntry.Key) {
		pi.Stop() // Nema promene, vratimo trenutni unos
		return rec, true
	}
	pi.currentEntry = newEntry
	for _, iterator := range pi.iterators {
		if iterator == nil || iterator.memtableIterator.currentEntry.Key == nil {
			continue
		}
		iterator.memtableIterator.currentEntry = newEntry
	}
	return rec, true
}

func (pi *PrefixIterator) Stop() {
	for _, iterator := range pi.iterators {
		if iterator != nil {
			iterator.Stop()
		}
	}
	pi.memtables = nil
	pi.iterators = nil
	pi.currentEntry = adapter.MemtableEntry{Key: nil}
}

type RangeIterator struct {
	memtables    *Memtables
	iterators    []*MemtableRangeIterator
	currentEntry adapter.MemtableEntry
	startKey     []byte
	endKey       []byte
}

func (ms *Memtables) RangeIterate(startKey, endKey []byte) *RangeIterator {
	if len(ms.Memtables) == 0 {
		return nil // Nema unosa u Memtables
	}
	iterators := make([]*MemtableRangeIterator, 0)
	for _, memtable := range ms.Memtables {
		it := memtable.RangeIterate(startKey, endKey)
		if it != nil {
			iterators = append(iterators, it)
		}
	}
	if len(iterators) == 0 {
		return nil
	}
	minKey := []byte{}
	currentEntry := adapter.MemtableEntry{Key: nil}
	for i := 0; i < len(iterators); i++ {
		if iterators[i] != nil {
			if len(minKey) == 0 {
				minKey = iterators[i].memtableIterator.currentEntry.Key
				currentEntry = iterators[i].memtableIterator.currentEntry
			}
			if bytes.Compare(iterators[i].memtableIterator.currentEntry.Key, minKey) < 0 {
				minKey = iterators[i].memtableIterator.currentEntry.Key
				currentEntry = iterators[i].memtableIterator.currentEntry
			}
			if bytes.Equal(iterators[i].memtableIterator.currentEntry.Key, minKey) {
				if iterators[i].memtableIterator.currentEntry.Timestamp > currentEntry.Timestamp {
					minKey = iterators[i].memtableIterator.currentEntry.Key
					currentEntry = iterators[i].memtableIterator.currentEntry
				}
			}
		}
	}
	if len(minKey) == 0 {
		return nil
	}

	for i := 0; i < len(iterators); i++ {
		if iterators[i] != nil {
			iterators[i].memtableIterator.currentEntry = currentEntry
		}
	}
	return &RangeIterator{
		memtables:    ms,
		iterators:    iterators,
		currentEntry: currentEntry,
		startKey:     startKey,
		endKey:       endKey,
	}
}
func (ri *RangeIterator) Next() (adapter.MemtableEntry, bool) {
	if ri.currentEntry.Key == nil {
		return adapter.MemtableEntry{}, false // Nema vise unosa
	}
	rec := ri.currentEntry
	newEntry := ri.currentEntry
	minKey := ri.currentEntry.Key
	iIndex := -1
	for i, iterator := range ri.iterators {
		if iterator == nil {
			continue
		}
		_, found := iterator.Next()
		key := iterator.memtableIterator.currentEntry
		if !found {
			continue // Nema vise unosa u ovom iteratoru
		}
		if key.Key == nil {
			continue // Nema vise unosa u ovom iteratoru
		}
		if bytes.Equal(ri.currentEntry.Key, minKey) {
			minKey = key.Key
			iIndex = i
			newEntry = key
		}

		if bytes.Compare(iterator.memtableIterator.currentEntry.Key, minKey) < 0 {
			minKey = key.Key
			iIndex = i
			newEntry = key
		} else if bytes.Equal(key.Key, minKey) {
			if key.Timestamp > newEntry.Timestamp {
				minKey = key.Key
				iIndex = i
				newEntry = key
			}
		}
	}
	if iIndex == -1 {
		ri.Stop() // Zatvaranje iteratora ako nema vise unosa
	}
	if bytes.Equal(rec.Key, newEntry.Key) {
		ri.Stop() // Nema promene, vratimo trenutni unos
		return rec, true
	}
	ri.currentEntry = newEntry
	if bytes.Compare(newEntry.Key, ri.endKey) > 0 {
		ri.Stop() // Zatvaranje iteratora ako je novi unos van opsega
		return rec, true
	}

	for _, iterator := range ri.iterators {
		if iterator == nil || iterator.memtableIterator.currentEntry.Key == nil {
			continue
		}
		iterator.memtableIterator.currentEntry = newEntry
	}
	return rec, true
}

func (ri *RangeIterator) Stop() {
	for _, iterator := range ri.iterators {
		if iterator != nil {
			iterator.Stop()
		}
	}
	ri.memtables = nil
	ri.iterators = nil
	ri.currentEntry = adapter.MemtableEntry{Key: nil}
}
