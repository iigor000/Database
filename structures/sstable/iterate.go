package sstable

import (
	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/structures/block_organization"
)

type SSTableIterator struct {
	sstable         *SSTable
	currentRecord   adapter.MemtableEntry
	nextBlockNumber int
	blockManager    *block_organization.BlockManager
}

func (sst *SSTable) NewSSTableIterator(bm *block_organization.BlockManager) *SSTableIterator {
	rec, nxtBlck := sst.Data.ReadRecord(bm, 0, sst.CompressionKey)
	println("SSTableIterator initialized with first record key:", string(rec.Key))
	return &SSTableIterator{
		sstable:         sst,
		currentRecord:   rec,
		nextBlockNumber: nxtBlck,
		blockManager:    bm,
	}
}

func (si *SSTableIterator) Next() (adapter.MemtableEntry, bool) {
	if si.currentRecord.Key == nil {
		return adapter.MemtableEntry{}, false // Nema vise zapisa
	}

	rec := si.currentRecord
	println("Current Record Key: ", string(si.currentRecord.Key))
	si.currentRecord, si.nextBlockNumber = si.sstable.Data.ReadRecord(si.blockManager, si.nextBlockNumber, si.sstable.CompressionKey)

	if si.currentRecord.Key == nil {
		si.Stop() // Zatvaranje iteratora ako nema vise zapisa
	}

	return rec, true
}

func (si *SSTableIterator) Stop() {
	si.blockManager = nil
	si.sstable = nil
	si.currentRecord = adapter.MemtableEntry{}
	si.nextBlockNumber = -1
}

type PrefixIterator struct {
	Iterator *SSTableIterator
	Prefix   string
}

// Inicijalizuje iterator koji vraca samo zapise sa datim prefiksom
func (sst *SSTable) PrefixIterate(prefix string, bm *block_organization.BlockManager) *PrefixIterator {
	if len(prefix) == 0 {
		it := sst.NewSSTableIterator(bm) // Ako je prefiks prazan, vracamo iterator koji sadrzi sve zapise
		return &PrefixIterator{
			Iterator: it,
			Prefix:   prefix,
		}
	}
	println("Creating Prefix iterator for prefix:", prefix)
	it := SSTableIterator{}
	it.sstable = sst
	it.blockManager = bm
	// Inicijalizujemo iterator sa prvim zapisom koji odgovara prefiksu
	rec, nextBlock := sst.ReadRecordWithKey(bm, 0, prefix)

	println("First record with prefix:", string(rec.Key))
	it.currentRecord = rec
	it.nextBlockNumber = nextBlock
	if it.currentRecord.Key == nil {
		it.Stop() // Ako nema zapisa sa tim prefiksom, zatvaramo iterator
		return nil
	}
	return &PrefixIterator{
		Iterator: &it,
		Prefix:   prefix,
	}
}
func (pi *PrefixIterator) Next() (adapter.MemtableEntry, bool) {
	if pi.Iterator.currentRecord.Key == nil {
		return adapter.MemtableEntry{}, false
	}
	record := pi.Iterator.currentRecord
	rec, nextBlock := pi.Iterator.sstable.ReadRecordWithKey(pi.Iterator.blockManager, pi.Iterator.nextBlockNumber, pi.Prefix)
	pi.Iterator.currentRecord = rec
	pi.Iterator.nextBlockNumber = nextBlock
	if nextBlock == -1 {
		pi.Iterator.Stop() // Zatvaramo iterator ako nema vise zapisa sa tim prefiksom
		return adapter.MemtableEntry{}, false
	}

	return record, true
}

type RangeIterator struct {
	Iterator *SSTableIterator
	StartKey string
	EndKey   string
}

// Inicijalizuje iterator koji vraca samo zapise u datom opsegu kljuceva
func (sst *SSTable) RangeIterate(startKey, endKey string, bm *block_organization.BlockManager) *RangeIterator {
	if startKey > endKey {
		return nil // Nevalidan opseg
	}
	it := &SSTableIterator{}
	it.sstable = sst
	it.blockManager = bm
	rec, nextBlock := sst.ReadRecordWithKey(bm, 0, startKey)
	it.currentRecord = rec
	it.nextBlockNumber = nextBlock
	if it.currentRecord.Key == nil {
		it.Stop() // Ako nema zapisa u tom opsegu, zatvaramo iterator
		return nil
	}
	return &RangeIterator{
		Iterator: it,
		StartKey: startKey,
		EndKey:   endKey,
	}
}

func (ri *RangeIterator) Next() (adapter.MemtableEntry, bool) {
	if ri.Iterator.currentRecord.Key == nil {
		return adapter.MemtableEntry{}, false // Nema vise zapisa
	}
	record := ri.Iterator.currentRecord
	rec, nextBlock := ri.Iterator.sstable.Data.ReadRecord(ri.Iterator.blockManager, ri.Iterator.nextBlockNumber, ri.Iterator.sstable.CompressionKey)
	ri.Iterator.currentRecord = rec
	ri.Iterator.nextBlockNumber = nextBlock

	if nextBlock == -1 || string(rec.Key) > ri.EndKey {
		ri.Iterator.Stop() // Zatvaramo iterator ako nema vise zapisa u opsegu
		return adapter.MemtableEntry{}, false
	}

	return record, true
}
