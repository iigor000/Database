package lsmtree

import (
	"fmt"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/structures/block_organization"
	"github.com/iigor000/database/structures/sstable"
)

// GenericIterator se koristi kao apstrakcija za iteratore koji se koriste u LSM stablu
type GenericIterator interface {
	HasNext() bool
	Next() (*adapter.MemtableEntry, error)
	Close()
}

type PrefixIteratorAdapter struct {
	iter *sstable.PrefixIterator
}

func (p *PrefixIteratorAdapter) HasNext() bool {
	return p.iter.HasNext()
}

func (p *PrefixIteratorAdapter) Next() (*adapter.MemtableEntry, error) {
	entry, found := p.iter.Next()
	if !found {
		return nil, nil
	}

	return &entry, nil
}

func (p *PrefixIteratorAdapter) Close() {
	p.iter.Close()
}

type RangeIteratorAdapter struct {
	iter *sstable.RangeIterator
}

func (r *RangeIteratorAdapter) HasNext() bool {
	return r.iter.HasNext()
}

func (r *RangeIteratorAdapter) Next() (*adapter.MemtableEntry, error) {
	entry, found := r.iter.Next()
	if !found {
		return nil, nil
	}

	return &entry, nil
}

func (r *RangeIteratorAdapter) Close() {
	r.iter.Close()
}

// PrefixScan pretražuje sve SSTable-ove u LSM stablu i vraća sve zapise koji počinju sa datim prefiksom
func PrefixScan(conf *config.Config, prefix string, cbm *block_organization.CachedBlockManager) ([]*sstable.DataRecord, error) {
	maxLevel := conf.LSMTree.MaxLevel
	seenKeys := make(map[string]bool)
	var allIters []GenericIterator

	for level := 1; level < maxLevel; level++ {
		refs, err := getSSTableReferences(conf, level, false) // najnoviji podaci prvo
		if err != nil {
			return nil, err
		}

		for _, ref := range refs {
			table, err := OpenSSTable(ref.Level, ref.Gen, conf, cbm)
			if err != nil {
				return nil, err
			}

			iter := table.PrefixIterate(prefix, cbm)
			if iter != nil {
				allIters = append(allIters, &PrefixIteratorAdapter{iter: iter})
			}
		}
	}

	if len(allIters) == 0 {
		return nil, nil
	}

	merged := NewMergedIterator(allIters...) // moraš da implementiraš ovaj tip iteracije
	defer merged.Close()

	var results []*sstable.DataRecord

	for merged.HasNext() {
		entry := merged.Next()
		if entry == nil {
			continue // nema više zapisa
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
func RangeScan(conf *config.Config, startKey, endKey string, cbm *block_organization.CachedBlockManager) ([]*sstable.DataRecord, error) {
	if startKey > endKey {
		return nil, fmt.Errorf("invalid key range: %s > %s", startKey, endKey)
	}

	maxLevel := conf.LSMTree.MaxLevel
	seenKeys := make(map[string]bool)
	var allIters []GenericIterator

	for level := 1; level < maxLevel; level++ {
		refs, err := getSSTableReferences(conf, level, false)
		if err != nil {
			return nil, err
		}

		for _, ref := range refs {
			table, err := OpenSSTable(ref.Level, ref.Gen, conf, cbm)
			if err != nil {
				return nil, err
			}

			iter := table.RangeIterate(startKey, endKey, cbm)
			if iter != nil {
				allIters = append(allIters, &RangeIteratorAdapter{iter: iter})
			}
		}
	}

	if len(allIters) == 0 {
		return nil, nil
	}

	merged := NewMergedIterator(allIters...) // takođe moraš implementirati
	defer merged.Close()

	var results []*sstable.DataRecord

	for merged.HasNext() {
		entry := merged.Next()
		keyStr := string(entry.Key)

		if seenKeys[keyStr] {
			continue
		}
		seenKeys[keyStr] = true

		if entry.Tombstone {
			continue
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
