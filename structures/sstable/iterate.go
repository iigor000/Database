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
	rec, nxtBlck := sst.Data.ReadRecord(bm, 0)
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
	si.currentRecord, si.nextBlockNumber = si.sstable.Data.ReadRecord(si.blockManager, si.nextBlockNumber)

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

func (sst *SSTable) PrefixIterate(prefix string, bm *block_organization.BlockManager) *SSTableIterator {
	if len(prefix) == 0 {
		it := sst.NewSSTableIterator(bm) // Ako je prefiks prazan, vracamo iterator koji sadrzi sve zapise
		return it
	}
	it := SSTableIterator{}
	it.sstable = sst
	it.blockManager = bm
	// Inicijalizujemo iterator sa prvim zapisom koji odgovara prefiksu
	rec, nextBlock := sst.ReadRecordWithPrefix(bm, 0, prefix)
	it.currentRecord = rec
	it.nextBlockNumber = nextBlock
	if it.currentRecord.Key == nil {
		it.Stop() // Ako nema zapisa sa tim prefiksom, zatvaramo iterator
		return nil
	}
	return &it
}
