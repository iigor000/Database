package sstable

import (
	"bytes"

	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/structures/block_organization"
)

type SSTableIterator struct {
	sstable         *SSTable
	CurrentRecord   adapter.MemtableEntry
	nextBlockNumber int
	blockManager    *block_organization.CachedBlockManager
}

func (sst *SSTable) NewSSTableIterator(bm *block_organization.CachedBlockManager) *SSTableIterator {
	bn := int(sst.Data.DataFile.Offset) / bm.BM.BlockSize
	rec, nxtBlck := sst.Data.ReadRecord(bm, bn, sst.CompressionKey)
	return &SSTableIterator{
		sstable:         sst,
		CurrentRecord:   rec,
		nextBlockNumber: nxtBlck,
		blockManager:    bm,
	}
}

func (si *SSTableIterator) Next() (adapter.MemtableEntry, bool) {
	if si.CurrentRecord.Key == nil {
		return adapter.MemtableEntry{}, false // Nema vise zapisa
	}
	rec := si.CurrentRecord
	if si.sstable.SingleFile {
		if int(si.sstable.Data.DataFile.SizeOnDisk) < si.nextBlockNumber*si.blockManager.BM.BlockSize {
			si.Stop()
			return rec, true
		}
	}
	si.CurrentRecord, si.nextBlockNumber = si.sstable.Data.ReadRecord(si.blockManager, si.nextBlockNumber, si.sstable.CompressionKey)

	if si.CurrentRecord.Key == nil {
		si.Stop() // Zatvaranje iteratora ako nema vise zapisa
	}

	return rec, true
}

func (si *SSTableIterator) Stop() {
	si.blockManager = nil
	si.sstable = nil
	si.CurrentRecord = adapter.MemtableEntry{Key: nil} // Oslobađanje trenutnog zapisa
	si.nextBlockNumber = -1
}

func (it *SSTableIterator) Peek() *adapter.MemtableEntry {
	if it.CurrentRecord.Key == nil {
		return nil
	}

	entryCopy := it.CurrentRecord
	return &entryCopy
}

type PrefixIterator struct {
	Iterator *SSTableIterator
	Prefix   string
}

// Inicijalizuje iterator koji vraca samo zapise sa datim prefiksom
func (sst *SSTable) PrefixIterate(prefix string, bm *block_organization.CachedBlockManager) *PrefixIterator {
	if len(prefix) == 0 {
		it := sst.NewSSTableIterator(bm) // Ako je prefiks prazan, vracamo iterator koji sadrzi sve zapise
		return &PrefixIterator{
			Iterator: it,
			Prefix:   prefix,
		}
	}
	it := SSTableIterator{}
	it.sstable = sst
	it.blockManager = bm
	// Inicijalizujemo iterator sa prvim zapisom koji odgovara prefiksu
	rec, nextBlock := sst.ReadRecordWithKey(bm, 0, prefix, false)
	it.CurrentRecord = rec
	it.nextBlockNumber = nextBlock

	if it.CurrentRecord.Key == nil {
		it.Stop() // Ako nema zapisa sa tim prefiksom, zatvaramo iterator
		return nil
	}
	return &PrefixIterator{
		Iterator: &it,
		Prefix:   prefix,
	}
}
func (pi *PrefixIterator) Next() (adapter.MemtableEntry, bool) {
	if pi.Iterator == nil {
		return adapter.MemtableEntry{}, false // Nema vise zapisa
	}
	if pi.Iterator.CurrentRecord.Key == nil {
		return adapter.MemtableEntry{}, false // Nema vise zapisa
	}
	record := pi.Iterator.CurrentRecord
	if pi.Iterator.sstable.SingleFile {
		if int(pi.Iterator.sstable.Data.DataFile.SizeOnDisk) < pi.Iterator.nextBlockNumber*pi.Iterator.blockManager.BM.BlockSize {
			pi.Stop()
			return record, true
		}
	}
	rec, nextBlock := pi.Iterator.sstable.Data.ReadRecord(pi.Iterator.blockManager, pi.Iterator.nextBlockNumber, pi.Iterator.sstable.CompressionKey)
	if !bytes.HasPrefix(rec.Key, []byte(pi.Prefix)) {
		pi.Stop() // Zatvaramo iterator ako nema vise zapisa sa tim prefiksom
		return record, true
	}
	pi.Iterator.CurrentRecord = rec
	pi.Iterator.nextBlockNumber = nextBlock

	if nextBlock == -1 {
		pi.Stop() // Zatvaramo iterator ako nema vise zapisa
		return adapter.MemtableEntry{}, false
	}

	return record, true
}

func (pi *PrefixIterator) Stop() {
	pi.Iterator.Stop()
	pi.Prefix = ""
	pi.Iterator.CurrentRecord = adapter.MemtableEntry{Key: nil} // Oslobodi trenutni zapis
}

type RangeIterator struct {
	Iterator *SSTableIterator
	StartKey string
	EndKey   string
}

// Inicijalizuje iterator koji vraca samo zapise u datom opsegu kljuceva
func (sst *SSTable) RangeIterate(startKey, endKey string, bm *block_organization.CachedBlockManager) *RangeIterator {
	if startKey > endKey {
		return nil // Nevalidan opseg
	}
	it := &SSTableIterator{}
	it.sstable = sst
	it.blockManager = bm
	rec, nextBlock := sst.ReadRecordWithKey(bm, 0, startKey, true)
	it.CurrentRecord = rec
	it.nextBlockNumber = nextBlock
	if it.CurrentRecord.Key == nil {
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
	if ri.Iterator.CurrentRecord.Key == nil {
		return adapter.MemtableEntry{}, false // Nema vise zapisa
	}
	record := ri.Iterator.CurrentRecord
	if ri.Iterator.sstable.SingleFile {
		if int(ri.Iterator.sstable.Data.DataFile.SizeOnDisk) < ri.Iterator.nextBlockNumber*ri.Iterator.blockManager.BM.BlockSize {
			ri.Stop()
			return record, false
		}
	}
	rec, nextBlock := ri.Iterator.sstable.Data.ReadRecord(ri.Iterator.blockManager, ri.Iterator.nextBlockNumber, ri.Iterator.sstable.CompressionKey)
	ri.Iterator.CurrentRecord = rec
	ri.Iterator.nextBlockNumber = nextBlock

	if nextBlock == -1 {
		ri.Stop() // Zatvaramo iterator ako nema vise zapisa u opsegu
		return adapter.MemtableEntry{}, false
	}
	if bytes.Compare(record.Key, []byte(ri.StartKey)) < 0 || bytes.Compare(record.Key, []byte(ri.EndKey)) > 0 {
		return ri.Next() // Preskacemo zapise koji nisu u opsegu
	}

	return record, true
}

func (ri *RangeIterator) Stop() {
	ri.Iterator.Stop()
	ri.StartKey = ""
	ri.EndKey = ""
	ri.Iterator.CurrentRecord = adapter.MemtableEntry{Key: nil}
}
