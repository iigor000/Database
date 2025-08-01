package lsmtree

import (
	"bytes"

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
		if iter != nil {
			iterators = append(iterators, iter)
		}
	}

	if len(iterators) == 0 {
		return nil
	}

	return &LSMTreeIterator{
		iterators:    iterators,
		CurrentEntry: nil, // Početno je nil, Next() će postaviti prvi validan
	}
}

func (l *LSMTreeIterator) Next() *adapter.MemtableEntry {
	for {
		var minKey []byte
		var minIndex int = -1

		// Pronađi iterator sa najmanjim trenutnim ključem
		for i, iter := range l.iterators {
			entry := iter.Peek()
			if entry == nil {
				continue
			}
			if minKey == nil || bytes.Compare(entry.Key, minKey) < 0 {
				minKey = entry.Key
				minIndex = i
			}
		}

		if minIndex == -1 {
			l.CurrentEntry = nil // Nema više validnih iteratora
			return nil
		}

		// Skupi sve zapise sa tim ključem i zadrži onaj sa najvećim timestamp-om
		var bestEntry *adapter.MemtableEntry
		var itersToAdvance []*sstable.SSTableIterator
		for _, iter := range l.iterators {
			entry := iter.Peek()
			if entry != nil && bytes.Equal(entry.Key, minKey) {
				if bestEntry == nil || entry.Timestamp > bestEntry.Timestamp {
					bestEntry = entry
				}
				itersToAdvance = append(itersToAdvance, iter)
			}
		}

		for _, iter := range itersToAdvance {
			iter.Next() // Pomeri iterator na sledeći element
		}

		if bestEntry != nil && !bestEntry.Tombstone {
			l.CurrentEntry = bestEntry
			return bestEntry
		}
	}
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

		if iter == nil {
			continue // Ako iterator nije uspeo da se kreira, preskoči ovaj SSTable
		}
		iterators = append(iterators, iter)
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
	if pi.CurrentRecord == nil {
		return nil // Nema više elemenata
	}
	if pi.CurrentRecord.Key == nil {
		pi.Stop() // Ako nema trenutnog zapisa, zaustavi iteraciju
		return nil
	}
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
			if iter.Iterator == nil {
				continue
			}
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
func PrefixScan(conf *config.Config, prefix string, cbm *block_organization.CachedBlockManager, dict *compression.Dictionary, pageNumber, pageSize int) ([]*sstable.DataRecord, error) {
	maxLevel := conf.LSMTree.MaxLevel
	//seenKeys := make(map[string]bool)
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
	index := pageNumber * pageSize
	endIndex := index + pageSize
	for i := 0; i < index; i++ {
		entry := merged.Next()
		if entry == nil {
			break // nema više zapisa
		}
	}
	for i := index; i < endIndex; i++ {
		entry := merged.Next()
		if entry == nil {
			break // nema više zapisa
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

type RangeIterator struct {
	iterators     []*sstable.RangeIterator
	CurrentRecord *adapter.MemtableEntry
	StartKey      string
	EndKey        string
}

func RangeIterate(tables []*sstable.SSTable, startKey, endKey string, bm *block_organization.CachedBlockManager) *RangeIterator {
	if startKey > endKey {
		return nil // Nevalidan opseg
	}

	iterators := make([]*sstable.RangeIterator, 0, len(tables))

	for _, sstable := range tables {
		iter := sstable.RangeIterate(startKey, endKey, bm)
		if iter == nil {
			continue // Ako iterator nije uspeo da se kreira, preskoči ovaj SSTable
		}
		iterators = append(iterators, iter)
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

	return &RangeIterator{
		iterators:     iterators,
		StartKey:      startKey,
		EndKey:        endKey,
		CurrentRecord: &minEntry, // Početno je nil, Next() će postaviti prvi validan
	}
}

func (ri *RangeIterator) Next() *adapter.MemtableEntry {

	if ri.CurrentRecord == nil {
		return nil // Nema više elemenata
	}
	if ri.CurrentRecord.Key == nil {
		ri.Stop() // Ako nema trenutnog zapisa, zaustavi iteraciju
		return nil
	}
	if len(ri.iterators) == 0 {
		return nil // Nema više elemenata
	}

	currentEntry := ri.CurrentRecord
	minEntry := adapter.MemtableEntry{Key: nil}
	for _, iter := range ri.iterators {
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
		ri.Stop()           // Ako nema više elemenata, zaustavi iteraciju
		return currentEntry // Nema više elemenata
	}

	for _, iter := range ri.iterators {
		if iter != nil {
			if iter.Iterator == nil {
				continue
			}
			if iter.Iterator.CurrentRecord.Key != nil {
				iter.Iterator.CurrentRecord = minEntry // Postavi trenutni zapis na najmanji ključ
			}
		}
	}

	return currentEntry
}

func (ri *RangeIterator) Stop() {
	for _, iter := range ri.iterators {
		if iter != nil {
			iter.Stop()
		}
	}
	ri.CurrentRecord = &adapter.MemtableEntry{Key: nil} // Oslobodi trenutni zapis

}

// RangeScan pretražuje sve SSTable-ove u LSM stablu i vraća sve zapise koji su unutar datog opsega ključeva
func RangeScan(conf *config.Config, startKey, endKey string, cbm *block_organization.CachedBlockManager, dict *compression.Dictionary, pageNumber, pageSize int) ([]*sstable.DataRecord, error) {
	maxLevel := conf.LSMTree.MaxLevel
	//seenKeys := make(map[string]bool)
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

	merged := RangeIterate(tables, startKey, endKey, cbm)

	if len(tables) == 0 {
		return nil, nil
	}

	var results []*sstable.DataRecord
	index := pageNumber * pageSize
	endIndex := index + pageSize
	for i := 0; i < index; i++ {
		entry := merged.Next()
		if entry == nil {
			break // nema više zapisa
		}
	}
	for i := index; i < endIndex; i++ {
		entry := merged.Next()
		if entry == nil {
			break // nema više zapisa
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
