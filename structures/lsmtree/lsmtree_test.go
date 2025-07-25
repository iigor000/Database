package lsmtree

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
			SstableDirectory: dataDir,
		},
		LSMTree: config.LSMTreeConfig{
			MaxLevel:            2,
			CompactionAlgorithm: "size_tiered",
			BaseSSTableLimit:    4,
			LevelSizeMultiplier: 2,
		},
	}

	err := Compact(cfg)
	if err != nil {
		t.Fatalf("Failed to compact LSM tree: %v", err)
	}
}
