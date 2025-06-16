package sstable

import (
	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/compression"
	"github.com/iigor000/database/structures/memtable"
	"github.com/iigor000/database/structures/merkle"
)

// SSTable struktura
type SSTable struct {
	Data    DataBlock
	Index   IndexBlock
	Summary SummaryBlock
	Filter  FilterBlock
	//TOC            map[string]int64
	Metadata       *merkle.MerkleTree
	Gen            int
	UseCompression bool
}

// NewSSTable kreira novi SSTable
func NewSSTable(conf *config.Config, memtable memtable.Memtable, generation int) *SSTable {
	var sstable SSTable
	sstable.UseCompression = conf.SSTable.UseCompression
	sstable.Gen = generation

	path := conf.SSTable.SstableDirectory + "/" + string(generation) + "/"

	sstable.Data = buildData(memtable, conf, generation, path)
	sstable.Index = buildIndex(conf, generation, path, sstable.Data)
	sstable.Summary = buildSummary(memtable)
	sstable.Filter = buildBloomFilter(memtable)
	//sstable.TOC = buildTOC(memtable)
	sstable.Metadata = buildMerkleTree(memtable)
	return &sstable
}

func buildData(mem memtable.Memtable, conf *config.Config, gen int, path string) DataBlock {
	var db DataBlock
	for i := 0; i < mem.Capacity; i++ {
		entry, found := mem.Structure.Search(mem.Keys[i])
		if found {
			dr := NewDataRecord(entry.Key, entry.Value, entry.Timestamp, entry.Tombstone)
			db.Records = append(db.Records, dr)
		}
	}
	dict := compression.NewDictionary()
	filename := CreateFileName(path, gen, "Data", "db")
	db.WriteData(filename, conf, dict)
	return db
}

func buildIndex(conf *config.Config, gen int, path string, db DataBlock) IndexBlock {
	var ib IndexBlock
	filename := CreateFileName(path, gen, "Index", "db")
	for _, record := range db.Records {
		ir := NewIndexRecord(record.Key, record.Offset)
		ib.Records = append(ib.Records, ir)
	}
	ib.WriteIndex(filename, conf)
	return ib
}

func buildSummary(memtable memtable.Memtable) SummaryBlock {
	var sb SummaryBlock

	return sb
}

func buildBloomFilter(memtable memtable.Memtable) FilterBlock {
	var fb FilterBlock

	return fb
}

func buildMerkleTree(memtable memtable.Memtable) *merkle.MerkleTree {
	var mt *merkle.MerkleTree

	return mt
}
