package sstable

import (
	"fmt"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/block_organization"
	"github.com/iigor000/database/structures/bloomfilter"
	"github.com/iigor000/database/structures/compression"
	"github.com/iigor000/database/structures/memtable"
	"github.com/iigor000/database/structures/merkle"
)

// SSTable struktura
type SSTable struct {
	Data           *Data
	Index          *Index
	Summary        SummaryBlock
	Filter         bloomfilter.BloomFilter
	Metadata       *merkle.MerkleTree
	Gen            int
	UseCompression bool
	CompressionKey *compression.Dictionary
}

// NewSSTable kreira novi SSTable
func FlushSSTable(conf *config.Config, memtable memtable.Memtable, generation int) *SSTable {
	var sstable SSTable
	sstable.UseCompression = conf.SSTable.UseCompression
	sstable.Gen = generation
	path := fmt.Sprintf("%s/%d", conf.SSTable.SstableDirectory, generation)
	err := CreateDirectoryIfNotExists(path)
	if err != nil {
		panic("Error creating directory for SSTable: " + err.Error())
	}
	sstable.Data, sstable.CompressionKey = buildData(memtable, conf, generation, path)
	sstable.Index = buildIndex(conf, generation, path, sstable.Data)
	//sstable.Summary = buildSummary(conf, sstable.Index)
	sstable.Filter = buildBloomFilter(conf, generation, path, sstable.Data)
	dictPath := CreateFileName(path, generation, "Dictionary", "db")
	sstable.CompressionKey.Write(dictPath)
	sstable.Metadata = buildMetadata(conf, generation, path, sstable.Data)
	// Upis TOC u fajl
	toc_path := CreateFileName(path, generation, "TOC", "txt")
	toc_data := fmt.Sprintf("Generation: %d\nData: %s\nIndex: %s\nFilter: %s\nMetadata: %s\n",
		sstable.Gen, CreateFileName(path, generation, "Data", "db"),
		CreateFileName(path, generation, "Index", "db"),
		CreateFileName(path, generation, "Filter", "db"),
		CreateFileName(path, generation, "Metadata", "db"))
	WriteTxtToFile(toc_path, toc_data)
	return &sstable
}

func buildData(mem memtable.Memtable, conf *config.Config, gen int, path string) (*Data, *compression.Dictionary) {
	db := &Data{}
	dict := compression.NewDictionary()
	for i := 0; i < mem.Capacity; i++ {
		entry, found := mem.Structure.Search(mem.Keys[i])
		if found {
			dr := NewDataRecord(entry.Key, entry.Value, entry.Timestamp, entry.Tombstone)
			db.Records = append(db.Records, dr)
			if conf.SSTable.UseCompression {
				dict.Add(entry.Key)
				dict.Add(entry.Value)
			}
		}
	}
	filename := CreateFileName(path, gen, "Data", "db")
	db.WriteData(filename, conf, dict)

	return db, dict
}

func buildIndex(conf *config.Config, gen int, path string, db *Data) *Index {
	ib := &Index{}
	filename := CreateFileName(path, gen, "Index", "db")
	for _, record := range db.Records {
		ir := NewIndexRecord(record.Key, record.Offset)
		ib.Records = append(ib.Records, ir)
	}
	ib.WriteIndex(filename, conf)
	return ib
}

func buildBloomFilter(conf *config.Config, gen int, path string, db *Data) bloomfilter.BloomFilter {
	filename := CreateFileName(path, gen, "Filter", "db")
	fb := bloomfilter.MakeBloomFilter(len(db.Records), 0.5)
	for _, record := range db.Records {
		fb.Add(record.Key)
		if !record.Tombstone {
			fb.Add(record.Value)
		}
	}
	serialized := fb.Serialize()
	bm := block_organization.NewBlockManager(conf)
	_, err := bm.AppendBlock(filename, serialized)
	if err != nil {
		panic("Error writing bloom filter to file: " + err.Error())
	}
	return fb
}

// buildMetadata kreira Merkle stablo i upisuje ga u fajl
func buildMetadata(conf *config.Config, gen int, path string, db *Data) *merkle.MerkleTree {
	filename := CreateFileName(path, gen, "Metadata", "db")
	data := make([][]byte, len(db.Records))
	for i, record := range db.Records {
		data[i] = record.Key
		if !record.Tombstone {
			data[i] = append(data[i], record.Value...)
		}
	}
	mt := merkle.NewMerkleTree(data)
	err := mt.SerializeToBinaryFile(filename)
	if err != nil {
		panic("Error writing Merkle tree to file: " + err.Error())
	}
	return mt
}

func ReadBloomFilter(path string, conf *config.Config) (bloomfilter.BloomFilter, error) {
	bm := block_organization.NewBlockManager(conf) // Koristimo nil jer nam nije potreban config ovde
	block, err := bm.ReadBlock(path, 0)
	if err != nil {
		return bloomfilter.BloomFilter{}, fmt.Errorf("error reading bloom filter from file %s: %w", path, err)
	}
	fb := bloomfilter.Deserialize(block)
	return fb[0], nil
}

func ReadMetadata(path string) (*merkle.MerkleTree, error) {
	mt, err := merkle.DeserializeFromBinaryFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading Merkle tree from file %s: %w", path, err)
	}
	return mt, nil
}

// NewSSTable kreira novi SSTable iz fajlova
func NewSSTable(dir string, conf *config.Config, gen int) *SSTable {
	sstable := &SSTable{
		CompressionKey: compression.NewDictionary(),
		Gen:            gen,
	}
	dir = fmt.Sprintf("%s/%d", dir, gen)
	// Proveravamo da li direktorijum postoji
	err := CreateDirectoryIfNotExists(dir)
	if err != nil {
		panic("Error creating directory for SSTable: " + err.Error())
	}
	// Citanje kompresije iz fajla
	dictPath := CreateFileName(dir, gen, "Dictionary", "db")
	dict, err := compression.Read(dictPath)
	if err != nil {
		panic("Error reading dictionary from file: " + err.Error())
	}
	if dict != nil {
		sstable.CompressionKey = dict
		sstable.UseCompression = true
	}
	sstable.UseCompression = false
	// Citanje Data fajla
	dataPath := CreateFileName(dir, gen, "Data", "db")
	data, err := ReadData(dataPath, conf, sstable.CompressionKey)
	if err != nil {
		panic("Error reading data from file: " + err.Error())
	}
	sstable.Data = data
	// Citanje Index fajla
	indexPath := CreateFileName(dir, gen, "Index", "db")
	index, err := ReadIndex(indexPath, conf)
	if err != nil {
		panic("Error reading index from file: " + err.Error())
	}
	sstable.Index = index
	// Citanje Filter fajla
	filterPath := CreateFileName(dir, gen, "Filter", "db")
	filterData, err := ReadBloomFilter(filterPath, conf)
	if err != nil {
		panic("Error reading bloom filter from file: " + err.Error())
	}
	sstable.Filter = filterData
	// Citanje Metadata fajla
	metadataPath := CreateFileName(dir, gen, "Metadata", "db")
	metadata, err := ReadMetadata(metadataPath)
	if err != nil {
		panic("Error reading Merkle tree from file: " + err.Error())
	}
	sstable.Metadata = metadata

	return sstable
}
