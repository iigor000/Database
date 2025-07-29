package lsmtree

import (
	"bytes"
	"fmt"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/structures/block_organization"
	"github.com/iigor000/database/structures/compression"
	"github.com/iigor000/database/structures/sstable"
)

type LSMTreeIterator struct {
	iterators    []*sstable.SSTableIterator
	CurrentEntry *adapter.MemtableEntry // trenutni zapis koji se koristi za iteraciju
}

func NewLSMTreeIterator(tables []*sstable.SSTable, bm *block_organization.CachedBlockManager) *LSMTreeIterator {
	iterators := make([]*sstable.SSTableIterator, 0, len(tables))

	for _, sstable := range tables {
		iter := sstable.NewSSTableIterator(bm)
		iterators = append(iterators, iter)

		if iter == nil {
			continue // Ako iterator nije uspeo da se kreira, preskoči ovaj SSTable
		}
	}

	if len(iterators) == 0 {
		return nil
	}

	minEntry := adapter.MemtableEntry{Key: nil}
	for _, iter := range iterators {
		it, ok := iter.Next()
		if !ok {
			continue // Ako iterator nema sledeći element, preskoči
		}

		if minEntry.Key == nil {
			minEntry = it
		}

		if bytes.Compare(it.Key, minEntry.Key) < 0 {
			minEntry = it // Pronađen je manji ključ, ažuriraj minEntry
		} else if bytes.Equal(minEntry.Key, it.Key) && minEntry.Timestamp < it.Timestamp {
			minEntry = it // Ažuriraj minEntry ako je timestamp veći
		}
	}

	for _, iter := range iterators {
		if iter != nil {
			if iter.CurrentRecord.Key != nil {
				iter.CurrentRecord = minEntry // Postavi trenutni zapis na najmanji ključ
			}
		}
	}

	return &LSMTreeIterator{
		CurrentEntry: &minEntry,
		iterators:    iterators,
	}
}

func (l *LSMTreeIterator) Next() *adapter.MemtableEntry {
	if len(l.iterators) == 0 {
		return nil // Nema više elemenata
	}

	currentEntry := l.CurrentEntry
	minEntry := adapter.MemtableEntry{Key: nil}
	for _, iter := range l.iterators {
		if iter == nil {
			continue // Ako iterator nije uspeo da se kreira, preskoči
		}

		it, ok := iter.Next()
		if !ok {
			continue // Ako iterator nema sledeći element, preskoči
		}

		if minEntry.Key == nil || bytes.Compare(it.Key, minEntry.Key) < 0 {
			minEntry = it // Pronađen je manji ključ, ažuriraj minEntry
		} else if minEntry.Timestamp < it.Timestamp {
			minEntry = it // Ažuriraj minEntry ako je timestamp veći
		}
	}

	if minEntry.Key == nil {
		l.Stop()            // Ako nema više elemenata, zaustavi iteraciju
		return currentEntry // Nema više elemenata
	}

	for _, iter := range l.iterators {
		if iter != nil {
			if iter.CurrentRecord.Key != nil {
				iter.CurrentRecord = minEntry // Postavi trenutni zapis na najmanji ključ
			}
		}
	}

	return currentEntry
}

func (l *LSMTreeIterator) Stop() {
	for _, iter := range l.iterators {
		if iter != nil {
			iter.Stop()
		}
	}
	l.CurrentEntry = &adapter.MemtableEntry{Key: nil} // Oslobodi trenutni zapis
	l.iterators = nil                                 // Oslobodi iteratore
}

type PrefixIterator struct {
	iterators     []*sstable.PrefixIterator
	CurrentRecord *adapter.MemtableEntry
	Prefix        string
}

func PrefixIterate(tables []*sstable.SSTable, conf *config.Config, prefix string, bm *block_organization.CachedBlockManager, dict *compression.Dictionary) *PrefixIterator {
	iterators := make([]*sstable.PrefixIterator, 0, len(tables))

	for _, sstable := range tables {
		iter := sstable.PrefixIterate(prefix, bm)
		iterators = append(iterators, iter)

		if iter == nil {
			continue // Ako iterator nije uspeo da se kreira, preskoči ovaj SSTable
		}
	}

	if len(iterators) == 0 {
		return nil
	}

	minEntry := adapter.MemtableEntry{Key: nil}
	for _, iter := range iterators {
		it, ok := iter.Next()
		if !ok {
			continue // Ako iterator nema sledeći element, preskoči
		}

		if minEntry.Key == nil {
			minEntry = it
		}

		if bytes.Compare(it.Key, minEntry.Key) < 0 {
			minEntry = it // Pronađen je manji ključ, ažuriraj minEntry
		} else if bytes.Equal(minEntry.Key, it.Key) && minEntry.Timestamp < it.Timestamp {
			minEntry = it // Ažuriraj minEntry ako je timestamp veći
		}
	}

	for _, iter := range iterators {
		if iter != nil {
			if iter.Iterator.CurrentRecord.Key != nil {
				iter.Iterator.CurrentRecord = minEntry // Postavi trenutni zapis na najmanji ključ
			}
		}
	}

	return &PrefixIterator{
		CurrentRecord: &minEntry,
		iterators:     iterators,
	}
}

func (pi *PrefixIterator) Next() *adapter.MemtableEntry {
	if len(pi.iterators) == 0 {
		return nil // Nema više elemenata
	}

	currentEntry := pi.CurrentRecord
	minEntry := adapter.MemtableEntry{Key: nil}
	for _, iter := range pi.iterators {
		if iter == nil {
			continue // Ako iterator nije uspeo da se kreira, preskoči
		}

		it, ok := iter.Next()
		if !ok {
			continue // Ako iterator nema sledeći element, preskoči
		}

		if minEntry.Key == nil || bytes.Compare(it.Key, minEntry.Key) < 0 {
			minEntry = it // Pronađen je manji ključ, ažuriraj minEntry
		} else if minEntry.Timestamp < it.Timestamp {
			minEntry = it // Ažuriraj minEntry ako je timestamp veći
		}
	}

	if minEntry.Key == nil {
		pi.Stop()           // Ako nema više elemenata, zaustavi iteraciju
		return currentEntry // Nema više elemenata
	}

	for _, iter := range pi.iterators {
		if iter != nil {
			if iter.Iterator.CurrentRecord.Key != nil {
				iter.Iterator.CurrentRecord = minEntry // Postavi trenutni zapis na najmanji ključ
			}
		}
	}

	return currentEntry
}

func (pi *PrefixIterator) Stop() {
	for _, iter := range pi.iterators {
		if iter != nil {
			iter.Stop()
		}
	}
	pi.CurrentRecord = &adapter.MemtableEntry{Key: nil} // Oslobodi trenutni zapis
	pi.iterators = nil                                  // Oslobodi iteratore
}

// PrefixScan pretražuje sve SSTable-ove u LSM stablu i vraća sve zapise koji počinju sa datim prefiksom
func PrefixScan(conf *config.Config, prefix string, cbm *block_organization.CachedBlockManager, dict *compression.Dictionary) ([]*sstable.DataRecord, error) {
	maxLevel := conf.LSMTree.MaxLevel
	seenKeys := make(map[string]bool)
	var tables []*sstable.SSTable

	for level := 1; level < maxLevel; level++ {
		refs, err := getSSTableReferences(conf, level, false) // najnoviji podaci prvo
		if err != nil {
			return nil, err
		}

		var tables []*sstable.SSTable

		for _, ref := range refs {
			table, err := sstable.StartSSTable(ref.Level, ref.Gen, conf, dict, cbm)
			if err != nil {
				return nil, err
			}

			tables = append(tables, table)
		}
	}

	merged := PrefixIterate(tables, conf, prefix, cbm, dict)

	if len(tables) == 0 {
		return nil, nil
	}

	var results []*sstable.DataRecord

	for {
		entry := merged.Next()
		if entry == nil {
			break // nema više zapisa
		}
		keyStr := string(entry.Key)
		if seenKeys[keyStr] {
			continue // već smo obradili ovaj ključ
		}
		seenKeys[keyStr] = true

		if entry.Tombstone {
			continue // obrisan podatak
		}

		results = append(results, &sstable.DataRecord{
			Key:       entry.Key,
			Value:     entry.Value,
			Timestamp: entry.Timestamp,
			Tombstone: entry.Tombstone,
		})
	}

	return results, nil
}

// RangeScan pretražuje sve SSTable-ove u LSM stablu i vraća sve zapise koji su unutar datog opsega ključeva
func RangeScan(conf *config.Config, startKey, endKey string, cbm *block_organization.CachedBlockManager, dict *compression.Dictionary) ([]*sstable.DataRecord, error) {
	if startKey > endKey {
		return nil, fmt.Errorf("invalid key range: %s > %s", startKey, endKey)
	}

	maxLevel := conf.LSMTree.MaxLevel
	//seenKeys := make(map[string]bool)
	var tables []*sstable.SSTable

	for level := 1; level < maxLevel; level++ {
		refs, err := getSSTableReferences(conf, level, false)
		if err != nil {
			return nil, err
		}

		for _, ref := range refs {
			table, err := sstable.StartSSTable(ref.Level, ref.Gen, conf, dict, cbm)
			if err != nil {
				return nil, err
			}

			tables = append(tables, table)

			// iter := table.RangeIterate(startKey, endKey, cbm)
			// if iter != nil {
			// 	allIters = append(allIters, &RangeIteratorAdapter{iter: iter})
			// }
		}
	}

	if len(tables) == 0 {
		return nil, nil
	}

	// merged := RangeIterate(tables, startKey, endKey, cbm)
	// if merged == nil {
	// 	return nil, nil
	// }

	// merged := NewMergedIterator(allIters...) // takođe moraš implementirati
	// defer merged.Close()

	// var results []*sstable.DataRecord

	// for merged.HasNext() {
	// 	entry := merged.Next()
	// 	keyStr := string(entry.Key)

	// 	if seenKeys[keyStr] {
	// 		continue
	// 	}
	// 	seenKeys[keyStr] = true

	// 	if entry.Tombstone {
	// 		continue
	// 	}

	// 	results = append(results, &sstable.DataRecord{
	// 		Key:       entry.Key,
	// 		Value:     entry.Value,
	// 		Timestamp: entry.Timestamp,
	// 		Tombstone: entry.Tombstone,
	// 	})
	// }

	// return results, nil
	return nil, nil
}
