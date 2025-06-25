package sstable

import (
	"testing"

	"github.com/iigor000/database/config"
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
			BlockSize:     4096,
			CacheCapacity: 100,
		},
	}
	// Initialize a memtable with some data
	memtable := memtable.NewMemtable(true, conf.Skiplist.MaxHeight, conf.Memtable.NumberOfEntries)
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

}

func TestSSTableFlush(t *testing.T) {
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
			BlockSize:     4096,
			CacheCapacity: 100,
		},
	}

	m := memtable.NewMemtables(conf)
	m.Update([]byte("key1"), []byte("value1"), 1, false)
	m.Update([]byte("key2"), []byte("value2"), 2, false)
	m.Update([]byte("key3"), []byte("value3"), 3, false)
	m.Update([]byte("key4"), []byte("value4"), 4, false)
	m.Update([]byte("key5"), []byte("value5"), 5, false)
}

func TestSSTableRead(t *testing.T) {
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
			BlockSize:     4096,
			CacheCapacity: 100,
		},
	}

	// Initialize a memtable with some data
	memtable := memtable.NewMemtable(true, conf.Skiplist.MaxHeight, conf.Memtable.NumberOfEntries)
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
}
