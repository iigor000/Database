package lmstree

import (
	"path/filepath"
	"testing"

	"github.com/iigor000/database/config"
)

func TestLSMTree_PutGetDelete(t *testing.T) {
	// Napravi privremeni direktorijum za test
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "sstable")

	cfg := &config.Config{
		Block: config.BlockConfig{
			BlockSize:     4096,
			CacheCapacity: 100,
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
			SstableDirectory: dataDir,
		},
		LSMTree: config.LSMTreeConfig{
			MaxLevel:               2,
			CompactionAlgorithm:    "size_tiered",
			UseSizeBasedCompaction: false,
			BaseLevelSizeMBLimit:   10,
			BaseSSTableLimit:       4,
			LevelSizeMultiplier:    2,
			MaxSSTablesPerLevel:    []int{4, 8},
		},
	}

	tree := NewLSMTree(cfg)

	// 1. Test Put & Get
	key := []byte("foo")
	val := []byte("bar")

	tree.Put(cfg, key, val)

	got, err := tree.Get(cfg, key)
	if err != nil {
		t.Fatalf("expected value, got error: %v", err)
	}
	if string(got) != "bar" {
		t.Errorf("expected value 'bar', got '%s'", string(got))
	}

	// 2. Trigger flush: upiši još jedan key
	tree.Put(cfg, []byte("baz"), []byte("qux"))

	// 3. Test Get nakon flush-a (iz SSTable-a)
	got, err = tree.Get(cfg, key)
	if err != nil {
		t.Fatalf("expected value after flush, got error: %v", err)
	}
	if string(got) != "bar" {
		t.Errorf("expected value 'bar' after flush, got '%s'", string(got))
	}

	// 4. Test Delete
	tree.Delete(cfg, key)

	// 5. Test Get posle brisanja (treba da vrati grešku)
	got, err = tree.Get(cfg, key)
	if err == nil {
		t.Errorf("expected error after delete, got value: %s", string(got))
	}
}
