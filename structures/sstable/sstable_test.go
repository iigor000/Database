package sstable

import (
	"testing"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/block_organization"
	"github.com/iigor000/database/structures/compression"
	"github.com/iigor000/database/structures/memtable"
)

func TestSSTable(t *testing.T) {
	println("Testing SSTable creation...")
	// Create a configuration for the SSTable
	conf := &config.Config{
		SSTable: config.SSTableConfig{
			SstableDirectory: "./sstable_test",
			UseCompression:   true,
			SummaryLevel:     2,
		},
		Memtable: config.MemtableConfig{
			NumberOfMemtables: 1,
			NumberOfEntries:   5,
			Structure:         "skiplist",
		},
		Skiplist: config.SkiplistConfig{
			MaxHeight: 16,
		},
		Block: config.BlockConfig{
			BlockSize: 4096,
		},
		Cache: config.CacheConfig{
			Capacity: 100,
		},
	}
	// Initialize a memtable with some data
	memtable := memtable.NewMemtable(conf)
	// Add some entries to the memtable
	memtable.Update([]byte("key1"), []byte("value1"), 1, false)
	memtable.Update([]byte("key2"), []byte("value2"), 2, false)
	memtable.Update([]byte("key3"), []byte("value3"), 3, false)
	memtable.Update([]byte("key4"), []byte("value4"), 4, false)
	memtable.Update([]byte("key5"), []byte("value5"), 5, false)
	println("Memtable entries:")
	memtable.Print()
	dict := compression.NewDictionary()
	dict.Add([]byte("key1"))
	dict.Add([]byte("key2"))
	dict.Add([]byte("key3"))
	dict.Add([]byte("key4"))
	dict.Add([]byte("key5"))

	bm := block_organization.NewBlockManager(conf)
	bc := block_organization.NewBlockCache(conf)
	cbm := &block_organization.CachedBlockManager{
		BM: bm,
		C:  bc,
	}
	// Flush the memtable to create an SSTable
	sstable := FlushSSTable(conf, *memtable, 1, dict, cbm)
	// Check if the SSTable has the expected number of records
	if len(sstable.Data.Records) != 5 {
		t.Errorf("Expected 5 records in SSTable, got %d", len(sstable.Data.Records))
	}
	// Print SSTable details
	println("SSTable created successfully with generation:", sstable.Gen)
	// Print Data Records
	for _, record := range sstable.Data.Records {
		println("Key:", string(record.Key), "Value:", string(record.Value), "Timestamp:", record.Timestamp, "Tombstone:", record.Tombstone)
	}
	// Print Index Records
	println("Index Records:")
	for _, record := range sstable.Index.Records {
		println("Key:", string(record.Key), "Offset:", record.Offset)
	}
	// Print Summary Records
	println("Summary Records:")
	for _, record := range sstable.Summary.Records {
		println("FirstKey:", string(record.FirstKey), "LastKey:", "IndexOffset:", record.IndexOffset, "NumberOfRecords:", record.NumberOfRecords)
	}

}

func TestSSTableRead(t *testing.T) {
	conf := &config.Config{
		SSTable: config.SSTableConfig{
			SstableDirectory: "./sstable_test",
			UseCompression:   true,
			SummaryLevel:     2,
			SingleFile:       true,
		},
		Memtable: config.MemtableConfig{
			NumberOfMemtables: 1,
			NumberOfEntries:   5,
			Structure:         "skiplist",
		},
		Skiplist: config.SkiplistConfig{
			MaxHeight: 16,
		},
		Block: config.BlockConfig{
			BlockSize: 4096,
		},
		Cache: config.CacheConfig{
			Capacity: 100,
		},
	}

	// Initialize a memtable with some data
	memtable := memtable.NewMemtable(conf)
	// Add some entries to the memtable

	memtable.Update([]byte("key5"), []byte("value5"), 5, false)
	memtable.Update([]byte("key1"), []byte("value1"), 1, false)
	memtable.Update([]byte("key2"), []byte("value2"), 2, false)
	memtable.Update([]byte("key3"), []byte("value3"), 3, false)
	memtable.Update([]byte("key4"), []byte("value4"), 4, false)
	println("Memtable entries:")
	memtable.Print()
	dict := compression.NewDictionary()
	dict.Add([]byte("key5"))
	dict.Add([]byte("key1"))
	dict.Add([]byte("key2"))
	dict.Add([]byte("key3"))
	dict.Add([]byte("key4"))

	bm := block_organization.NewBlockManager(conf)
	bc := block_organization.NewBlockCache(conf)
	cbm := &block_organization.CachedBlockManager{
		BM: bm,
		C:  bc,
	}
	sstable := FlushSSTable(conf, *memtable, 1, dict, cbm)
	if sstable == nil {
		t.Fatal("Failed to create SSTable")
	}
	println("SSTable created successfully with generation:", sstable.Gen)
	// Print SSTABLEREAD
	for _, record := range sstable.Data.Records {
		println("Key:", string(record.Key), "Value:", string(record.Value), "Timestamp:", record.Timestamp, "Tombstone:", record.Tombstone)
	}
	// Print Index Records
	println("Index Records:")
	for _, record := range sstable.Index.Records {
		println("Key:", string(record.Key), "Offset:", record.Offset)
	}
	// Print Summary Records
	println("Summary Records:")
	println("FirstKey:", string(sstable.Summary.FirstKey), "LastKey:", string(sstable.Summary.LastKey))
	for _, record := range sstable.Summary.Records {
		println("FirstKey:", string(record.FirstKey), "IndexOffset:", record.IndexOffset, "NumberOfRecords:", record.NumberOfRecords)
	}
	println("Testing SSTable read...")
	// Read the SSTable from disk
	readSSTable, err := StartSSTable(sstable.Level, sstable.Gen, conf, dict, cbm)
	if err != nil {
		t.Fatal("Failed to read SSTable:", err)
	}
	println("SSTable read successfully with generation:", readSSTable.Gen)

	// Print Summary Records
	println("Summary FirstKey:", string(readSSTable.Summary.FirstKey), "LastKey:", string(readSSTable.Summary.LastKey))
	println("Summary Records:")
	for _, record := range readSSTable.Summary.Records {
		println("FirstKey:", string(record.FirstKey), "IndexOffset:", record.IndexOffset, "NumberOfRecords:", record.NumberOfRecords)
	}

	dict.Write("./sstable_test/dict_test.db", cbm)

}

// TestSSTableIterate tests the iteration functionality of the SSTable
func TestSSTableIterate(t *testing.T) {
	conf := &config.Config{
		SSTable: config.SSTableConfig{
			SstableDirectory: "./sstable_test",
			UseCompression:   true,
			SummaryLevel:     2,
			SingleFile:       true,
		},
		Memtable: config.MemtableConfig{
			NumberOfMemtables: 1,
			NumberOfEntries:   5,
			Structure:         "skiplist",
		},
		Skiplist: config.SkiplistConfig{
			MaxHeight: 16,
		},
		Block: config.BlockConfig{
			BlockSize: 4096,
		},
		Cache: config.CacheConfig{
			Capacity: 100,
		},
	}

	bm := block_organization.NewBlockManager(conf)
	bc := block_organization.NewBlockCache(conf)
	cbm := &block_organization.CachedBlockManager{
		BM: bm,
		C:  bc,
	}
	dict, err := compression.Read("./sstable_test/dict_test.db", cbm)
	if err != nil {
		t.Fatalf("Failed to read dictionary: %v", err)
	}
	sstable, err := StartSSTable(1, 1, conf, dict, cbm)
	if err != nil {
		t.Fatalf("Failed to start SSTable: %v", err)
	}
	// Print Summary Records
	println("Summary Records:")
	for _, record := range sstable.Summary.Records {
		println("FirstKey:", string(record.FirstKey), "LastKey:", "IndexOffset:", record.IndexOffset, "NumberOfRecords:", record.NumberOfRecords)
	}
	it := sstable.NewSSTableIterator(cbm)
	println("Iterating over SSTable records:")
	for {
		entry, ok := it.Next()
		if !ok {
			break // No more records
		}
		println("Key:", string(entry.Key), "Value:", string(entry.Value), "Timestamp:", entry.Timestamp, "Tombstone:", entry.Tombstone)
		keyCopy := make([]byte, len(entry.Key))
		copy(keyCopy, entry.Key)

		found := sstable.Filter.Read(keyCopy)
		if !found {
			println("Key not found in filter:", string(entry.Key))
		} else {
			println("Key found in filter:", string(entry.Key))
		}
	}
	// Test PrefixIterate
	println("Testing PrefixIterate...")
	prefix := "key5"
	prefixIter := sstable.PrefixIterate(prefix, cbm)
	if prefixIter == nil {
		t.Fatal("Failed to create Prefix iterator")
	}
	println("Iterating over SSTable records with prefix:", prefix)
	for {
		entry, ok := prefixIter.Next()
		if !ok {
			break // No more records
		}
		println("Key:", string(entry.Key), "Value:", string(entry.Value), "Timestamp:", entry.Timestamp, "Tombstone:", entry.Tombstone)
	}
	// Test RangeIterate
	println("Testing RangeIterate key2-key4...")
	startKey := "key2"
	endKey := "key4"
	rangeIter := sstable.RangeIterate(startKey, endKey, cbm)
	if rangeIter == nil {
		t.Fatal("Failed to create Range iterator")
	}
	println("Iterating over SSTable records in range:", startKey, "-", endKey)
	for {
		entry, ok := rangeIter.Next()
		if !ok {
			break // No more records
		}
		println("Key:", string(entry.Key), "Value:", string(entry.Value), "Timestamp:", entry.Timestamp, "Tombstone:", entry.Tombstone)

	}
}

func TestSSTableScan(t *testing.T) {
	conf := &config.Config{
		SSTable: config.SSTableConfig{
			SstableDirectory: "./sstable_test",
			UseCompression:   true,
			SummaryLevel:     2,
			SingleFile:       true,
		},
		Memtable: config.MemtableConfig{
			NumberOfMemtables: 1,
			NumberOfEntries:   5,
			Structure:         "skiplist",
		},
		Skiplist: config.SkiplistConfig{
			MaxHeight: 16,
		},
		Block: config.BlockConfig{
			BlockSize: 4096,
		},
		Cache: config.CacheConfig{
			Capacity: 100,
		},
	}

	bm := block_organization.NewBlockManager(conf)
	bc := block_organization.NewBlockCache(conf)
	cbm := &block_organization.CachedBlockManager{
		BM: bm,
		C:  bc,
	}
	dict, err := compression.Read("./sstable_test/dict_test.db", cbm)
	if err != nil {
		t.Fatalf("Failed to read dictionary: %v", err)
	}
	sstable, err := StartSSTable(1, 1, conf, dict, cbm)
	if err != nil {
		t.Fatalf("Failed to start SSTable: %v", err)
	}
	prefix := "key1"
	println("Testing PrefixScan for prefix:", prefix)
	results, err := sstable.PrefixScan(prefix, 0, 10, conf, cbm)
	if err != nil {
		t.Fatalf("PrefixScan failed: %v", err)
	}
	for _, entry := range results {
		println("Key:", string(entry.Key), "Value:", string(entry.Value), "Timestamp:", entry.Timestamp, "Tombstone:", entry.Tombstone)
	}

	minKey := []byte("key2")
	maxKey := []byte("key4")
	println("Testing RangeScan for keys between", string(minKey), "and", string(maxKey))
	rangeResults, err := sstable.RangeScan(minKey, maxKey, 0, 10, conf, cbm)
	if err != nil {
		t.Fatalf("RangeScan failed: %v", err)
	}
	for _, entry := range rangeResults {
		println("Key:", string(entry.Key), "Value:", string(entry.Value), "Timestamp:", entry.Timestamp, "Tombstone:", entry.Tombstone)
	}
}

func TestSSTableValidate(t *testing.T) {
	conf := &config.Config{
		SSTable: config.SSTableConfig{
			SstableDirectory: "./sstable_test",
			UseCompression:   true,
			SummaryLevel:     2,
			SingleFile:       true,
		},
		Memtable: config.MemtableConfig{
			NumberOfMemtables: 1,
			NumberOfEntries:   5,
			Structure:         "skiplist",
		},
		Skiplist: config.SkiplistConfig{
			MaxHeight: 16,
		},
		Block: config.BlockConfig{
			BlockSize: 4096,
		},
		Cache: config.CacheConfig{
			Capacity: 100,
		},
	}

	bm := block_organization.NewBlockManager(conf)
	bc := block_organization.NewBlockCache(conf)
	cbm := &block_organization.CachedBlockManager{
		BM: bm,
		C:  bc,
	}
	dict, err := compression.Read("./sstable_test/dict_test.db", cbm)
	dict.Print()
	if err != nil {
		t.Fatalf("Failed to read dictionary: %v", err)
	}
	sstable, err := StartSSTable(1, 1, conf, dict, cbm)
	if err != nil {
		t.Fatalf("Failed to start SSTable: %v", err)
	}
	changed, err := sstable.ValidateMerkleTree(conf, dict, cbm)
	if err != nil {
		t.Fatalf("SSTable validation failed: %v", err)
	}
	if changed {
		t.Fatal("SSTable validation returned false")
	}
	println("SSTable validation passed successfully")
}
