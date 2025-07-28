package lsmtree

import (
	"fmt"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/structures/block_organization"
	"github.com/iigor000/database/structures/sstable"
)

// Koristi samo Data (za iteraciju i Merge operacije)
type SSTableIterator struct {
	data     *sstable.Data
	current  int // trenutni block number (offset u blokovima)
	endBlock int // blok gde treba da stane (ne uključujući)
	cbm      *block_organization.CachedBlockManager
	conf     *config.Config
}

// NewSSTableIterator kreira novi SSTableIterator za dati SSTableReference
func NewSSTableIterator(ref *SSTableReference, conf *config.Config, cbm *block_organization.CachedBlockManager) (*SSTableIterator, error) {
	dir := fmt.Sprintf("%s/%d/%d", conf.SSTable.SstableDirectory, ref.Level, ref.Gen)

	dataFile := sstable.File{
		Path:       sstable.CreateFileName(dir, ref.Gen, "Data", ".db"),
		Offset:     0,
		SizeOnDisk: -1,
	}
	startBlock := 0
	endBlock := -1 // nepoznato još

	if conf.SSTable.SingleFile {
		path := sstable.CreateFileName(dir, ref.Gen, "SSTable", ".db")
		offsets, err := sstable.ReadOffsetsFromFile(path, conf, cbm)
		if err != nil {
			return nil, err
		}
		dataFile.Path = path
		dataFile.Offset = offsets["Data"]
		dataFile.SizeOnDisk = offsets["Index"] - offsets["Data"]

		startBlock = int(offsets["Data"] / int64(cbm.BM.BlockSize))
		endBlock = int(offsets["Index"] / int64(cbm.BM.BlockSize))
	} else {
		startBlock = 0
		endBlock = -1 // znači da ide do kraja fajla (ako je poznato, može se postaviti)
	}

	data := &sstable.Data{
		DataFile: dataFile,
	}

	return &SSTableIterator{
		data:     data,
		current:  startBlock,
		endBlock: endBlock,
		cbm:      cbm,
		conf:     conf,
	}, nil
}

func (it *SSTableIterator) Valid() bool {
	if it.endBlock < 0 {
		return true // nepoznata dužina, pokušaj dalje
	}
	return it.current < it.endBlock
}

func (it *SSTableIterator) HasNext() bool {
	if it.current < 0 {
		return false
	}
	if it.endBlock >= 0 && it.current >= it.endBlock {
		return false
	}

	// Ako imamo veličinu fajla poznatu, proveri da nismo iza kraja fajla
	if it.data.DataFile.SizeOnDisk > 0 {
		maxBlock := int(it.data.DataFile.SizeOnDisk) / it.cbm.BM.BlockSize
		if it.current >= maxBlock {
			return false
		}
	}

	return true
}

// Next vraća sledeći MemtableEntry iz SSTableIterator-a
func (it *SSTableIterator) Next() (adapter.MemtableEntry, error) {
	if !it.Valid() {
		return adapter.MemtableEntry{}, fmt.Errorf("no more records")
	}

	rec, nextBlock := it.data.ReadRecord(it.cbm, it.current, nil)
	it.current = nextBlock

	if rec.Key == nil || it.current > it.endBlock && it.endBlock >= 0 {
		return adapter.MemtableEntry{}, fmt.Errorf("no more records")
	}

	return rec, nil
}

func (it *SSTableIterator) Close() {
	it.cbm = nil
	it.data = nil
	it.current = -1
	it.endBlock = -1
	it.conf = nil
}

type SSTableIteratorAdapter struct {
	iter *SSTableIterator
}

func (s *SSTableIteratorAdapter) HasNext() bool {
	return s.iter.HasNext()
}

func (s *SSTableIteratorAdapter) Next() (*adapter.MemtableEntry, error) {
	if !s.iter.HasNext() {
		return nil, fmt.Errorf("no more records")
	}
	rec, err := s.iter.Next()
	if err != nil {
		return nil, err
	}
	return &rec, nil
}

func (s *SSTableIteratorAdapter) Close() {
	s.iter.Close()
	s.iter = nil
}
