package sstable

import (
	"testing"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/block_organization"
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
	}
	// Initialize a memtable with some data
	memtable := memtable.NewMemtable(conf, conf.Memtable.NumberOfEntries)
	// Add some entries to the memtable
	memtable.Update([]byte("key1"), []byte("value1"), 1, false)
	memtable.Update([]byte("key2"), []byte("value2"), 2, false)
	memtable.Update([]byte("key3"), []byte("value3"), 3, false)
	memtable.Update([]byte("key4"), []byte("value4"), 4, false)
	memtable.Update([]byte("key5"), []byte("value5"), 5, false)
	println("Memtable entries:")
	memtable.Print()
	// Flush the memtable to create an SSTable
	sstable := FlushSSTable(conf, *memtable, 1)
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
			UseCompression:   false,
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
	}

	// Initialize a memtable with some data
	memtable := memtable.NewMemtable(conf, conf.Memtable.NumberOfEntries)
	// Add some entries to the memtable
	memtable.Update([]byte("key1"), []byte("value1"), 1, false)
	memtable.Update([]byte("key2"), []byte("value2"), 2, false)
	memtable.Update([]byte("key3"), []byte("value3"), 3, false)
	memtable.Update([]byte("key4"), []byte("value4"), 4, false)
	memtable.Update([]byte("key5"), []byte("value5"), 5, false)
	println("Memtable entries:")
	memtable.Print()
	sstable := FlushSSTable(conf, *memtable, 1)
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
	readSSTable := NewSSTable(conf.SSTable.SstableDirectory, conf, sstable.Gen)
	if readSSTable == nil {
		t.Fatal("ReadSSTable returned nil")
	}
	println("SSTable read successfully with generation:", readSSTable.Gen)
	// Print Data Records
	for _, record := range readSSTable.Data.Records {
		println("Key:", string(record.Key), "Value:", string(record.Value), "Timestamp:", record.Timestamp, "Tombstone:", record.Tombstone)
	}
	// Print Index Records
	println("Index Records:")
	for _, record := range readSSTable.Index.Records {
		println("Key:", string(record.Key), "Offset:", record.Offset)
	}
	// Print Summary Records
	println("Summary FirstKey:", string(readSSTable.Summary.FirstKey), "LastKey:", string(readSSTable.Summary.LastKey))
	println("Summary Records:")
	for _, record := range readSSTable.Summary.Records {
		println("FirstKey:", string(record.FirstKey), "IndexOffset:", record.IndexOffset, "NumberOfRecords:", record.NumberOfRecords)
	}

}

// TestSSTableIterate tests the iteration functionality of the SSTable
func TestSSTableIterate(t *testing.T) {
	conf := &config.Config{
		SSTable: config.SSTableConfig{
			SstableDirectory: "./sstable_test",
			UseCompression:   false,
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
	}
	sstable, err := StartSSTable(1, conf)
	if err != nil {
		t.Fatalf("Failed to start SSTable: %v", err)
	}
	// Print Summary Records
	println("Summary Records:")
	for _, record := range sstable.Summary.Records {
		println("FirstKey:", string(record.FirstKey), "LastKey:", "IndexOffset:", record.IndexOffset, "NumberOfRecords:", record.NumberOfRecords)
	}
	bm := block_organization.NewBlockManager(conf)
	it := sstable.NewSSTableIterator(bm)
	println("Iterating over SSTable records:")
	for {
		entry, ok := it.Next()
		if !ok {
			break // No more records
		}
		println("Key:", string(entry.Key), "Value:", string(entry.Value), "Timestamp:", entry.Timestamp, "Tombstone:", entry.Tombstone)
	}
	// Test PrefixIterate
	println("Testing PrefixIterate...")
	prefix := "key1"
	prefixIter := sstable.PrefixIterate(prefix, bm)
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
	rangeIter := sstable.RangeIterate(startKey, endKey, bm)
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
			UseCompression:   false,
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
	}
	sstable, err := StartSSTable(1, conf)
	if err != nil {
		t.Fatalf("Failed to start SSTable: %v", err)
	}
	prefix := "key1"
	println("Testing PrefixScan for prefix:", prefix)
	results, err := sstable.PrefixScan(prefix, 0, 10, conf)
	if err != nil {
		t.Fatalf("PrefixScan failed: %v", err)
	}
	for _, entry := range results {
		println("Key:", string(entry.Key), "Value:", string(entry.Value), "Timestamp:", entry.Timestamp, "Tombstone:", entry.Tombstone)
	}

	minKey := []byte("key2")
	maxKey := []byte("key4")
	println("Testing RangeScan for keys between", string(minKey), "and", string(maxKey))
	rangeResults, err := sstable.RangeScan(minKey, maxKey, 0, 10, conf)
	if err != nil {
		t.Fatalf("RangeScan failed: %v", err)
	}
	for _, entry := range rangeResults {
		println("Key:", string(entry.Key), "Value:", string(entry.Value), "Timestamp:", entry.Timestamp, "Tombstone:", entry.Tombstone)
	}
}
