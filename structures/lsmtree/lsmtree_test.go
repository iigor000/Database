package lsmtree

import (
	"testing"
	"time"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/sstable"
)

func createTestConfig(baseDir string) *config.Config {
	return &config.Config{
		Block: config.BlockConfig{
			BlockSize: 4096,
		},
		Cache: config.CacheConfig{
			Capacity: 10,
		},
		Memtable: config.MemtableConfig{
			NumberOfMemtables: 1,
			NumberOfEntries:   2, // forsira flush nakon 2 upisa
			Structure:         "skiplist",
		},
		SSTable: config.SSTableConfig{
			UseCompression:   false,
			SummaryLevel:     1,
			SstableDirectory: baseDir,
		},
		LSMTree: config.LSMTreeConfig{
			MaxLevel:            2,
			CompactionAlgorithm: "size_tiered",
			BaseSSTableLimit:    4,
			LevelSizeMultiplier: 2,
		},
	}
}

func createSSTableWithData(t *testing.T, conf *config.Config, level int, gen int, key string, value string) {
	err := buildSSTableFromRecords([]*sstable.DataRecord{
		{Key: []byte(key), Value: []byte(value), Timestamp: time.Now().UnixNano()},
	}, conf, level, gen)
	if err != nil {
		t.Fatalf("Failed to create SSTable: %v", err)
	}
}

func TestGetExistingKey(t *testing.T) {
	tmp := t.TempDir()
	conf := createTestConfig(tmp)

	// Napravi SSTable sa jednim zapisom
	createSSTableWithData(t, conf, 1, 1, "key1", "value1")

	rec, err := Get(conf, []byte("key1"))
	if err != nil {
		t.Errorf("Unexpected error in Get: %v", err)
	}
	if rec == nil || string(rec.Value) != "value1" {
		t.Errorf("Expected value1, got %v", rec)
	}
}

func TestGetMissingKey(t *testing.T) {
	tmp := t.TempDir()
	conf := createTestConfig(tmp)

	// Napravi SSTable sa jednim zapisom
	createSSTableWithData(t, conf, 1, 1, "key1", "value1")

	rec, err := Get(conf, []byte("notfound"))
	if err != nil {
		t.Errorf("Unexpected error in Get: %v", err)
	}
	if rec != nil {
		t.Errorf("Expected nil, got %+v", rec)
	}
}

func TestCompact(t *testing.T) {
	tmp := t.TempDir()
	conf := createTestConfig(tmp)

	// Napravi dva SSTable sa različitim ključevima
	createSSTableWithData(t, conf, 1, 1, "key1", "value1")
	createSSTableWithData(t, conf, 1, 2, "key2", "value2")

	err := Compact(conf)
	if err != nil {
		t.Errorf("Compact failed: %v", err)
	}

	// Get bi trebalo da radi i nakon kompakcije
	rec1, _ := Get(conf, []byte("key1"))
	if rec1 == nil || string(rec1.Value) != "value1" {
		t.Errorf("Expected key1 to return value1, got %+v", rec1)
	}

	rec2, _ := Get(conf, []byte("key2"))
	if rec2 == nil || string(rec2.Value) != "value2" {
		t.Errorf("Expected key2 to return value2, got %+v", rec2)
	}
}

func TestMergeKeepsLatestTimestamp(t *testing.T) {
	tmp := t.TempDir()
	conf := createTestConfig(tmp)

	now := time.Now().UnixNano()
	old := now - 1000

	// Napravi SSTable sa DataRecord sa starijim timestampom
	err := buildSSTableFromRecords([]*sstable.DataRecord{
		{Key: []byte("dup"), Value: []byte("old"), Timestamp: old},
	}, conf, 1, 1)
	if err != nil {
		t.Fatalf("Failed to create SSTable: %v", err)
	}

	// Napravi SSTable sa DataRecord sa novijim timestampom
	err = buildSSTableFromRecords([]*sstable.DataRecord{
		{Key: []byte("dup"), Value: []byte("new"), Timestamp: now},
	}, conf, 1, 2)
	if err != nil {
		t.Fatalf("Failed to create SSTable: %v", err)
	}

	err = Compact(conf)
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	rec, err := Get(conf, []byte("dup"))
	if err != nil {
		t.Errorf("Get failed after merge: %v", err)
	}
	if rec == nil || string(rec.Value) != "new" {
		t.Errorf("Expected value to be 'new', got %+v", rec)
	}
}

func TestGetOverlappingSSTables(t *testing.T) {
	tmp := t.TempDir()
	conf := createTestConfig(tmp)

	// Napravi dva SSTable sa preklapajućim ključevima
	createSSTableWithData(t, conf, 1, 1, "key1", "value1")
	createSSTableWithData(t, conf, 1, 2, "key1", "value2")
	rec, err := Get(conf, []byte("key1"))
	if err != nil {
		t.Errorf("Unexpected error in Get: %v", err)
	}
	if rec == nil || string(rec.Value) != "value2" {
		t.Errorf("Expected value2, got %v", rec)
	}
}
