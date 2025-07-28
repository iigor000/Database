package lsmtree

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/structures/block_organization"
	"github.com/iigor000/database/structures/compression"
	"github.com/iigor000/database/structures/sstable"
)

var cbm *block_organization.CachedBlockManager

func createTestConfig(t *testing.T) *config.Config {
	dir := t.TempDir()

	conf := &config.Config{
		Block: config.BlockConfig{
			BlockSize: 4096,
		},
		Wal: config.WalConfig{
			WalSegmentSize: 100,
			WalDirectory:   "data",
		},
		Memtable: config.MemtableConfig{
			NumberOfMemtables: 1,
			NumberOfEntries:   2,
			Structure:         "skiplist",
		},
		Skiplist: config.SkiplistConfig{
			MaxHeight: 16,
		},
		BTree: config.BTreeConfig{
			MinSize: 16,
		},
		SSTable: config.SSTableConfig{
			UseCompression:   false,
			SummaryLevel:     10,
			SstableDirectory: dir,
			SingleFile:       true,
		},
		Cache: config.CacheConfig{
			Capacity: 100,
		},
		LSMTree: config.LSMTreeConfig{
			MaxLevel:            3,
			CompactionAlgorithm: "size_tiered",
			// "leveled" KOMPAKCIJA
			BaseSSTableLimit:    1024 * 1024, // 1MB - Bazni limit SSTable-a (DataBlock je velicine 4096, 8192 ili 16384 bajta)
			LevelSizeMultiplier: 10,          // Multiplikator velicine nivoa (granica za prvi nivo je BaseSSTableLimit pomnožena sa 10, kod drugog sa 100, itd.)
			// "size_tiered" KOMPAKCIJA
			MaxTablesPerLevel: 4, // Maksimalan broj SSTable-ova po nivou
		},
		TokenBucket: config.TokenBucketConfig{
			StartTokens:     1000, // Broj tokena na pocetku
			RefillIntervalS: 120,  // Interval refilovanja tokena u sekundama
		},
		Compression: config.CompressionConfig{
			DictionaryDir: "data/compression_dict",
		},
	}

	bm := block_organization.NewBlockManager(conf)
	bc := block_organization.NewBlockCache(conf)
	cbm = &block_organization.CachedBlockManager{
		BM: bm,
		C:  bc,
	}

	return conf
}

// Helper: Kreira i upisuje SSTable sa jednim zapisom za test
func createTestSSTable(t *testing.T, conf *config.Config, level int, gen int, key, value []byte, dict *compression.Dictionary) *SSTableReference {
	t.Helper()
	ref := &SSTableReference{Level: level, Gen: gen}

	// Kreiraj jednostavan SSTable builder i upiši jedan zapis
	builder, err := NewSSTableBuilder(level, gen, conf)
	if err != nil {
		t.Fatalf("failed to create SSTable builder: %v", err)
	}

	err = builder.Write(adapter.MemtableEntry{Key: key, Value: value, Timestamp: 1, Tombstone: false})
	if err != nil {
		t.Fatalf("failed to write record: %v", err)
	}

	err = builder.Finish(cbm, dict)
	if err != nil {
		t.Fatalf("failed to finish SSTable build: %v", err)
	}

	return ref
}

func TestGetAndCompact(t *testing.T) {
	conf := createTestConfig(t)
	dict := compression.NewDictionary()

	dict.Add([]byte("key1"))
	dict.Add([]byte("key2"))

	// Napravi 2 SSTable-ova sa različitim ključevima na nivou 1
	createTestSSTable(t, conf, 1, 1, []byte("key1"), []byte("value1"), dict)
	createTestSSTable(t, conf, 1, 2, []byte("key2"), []byte("value2"), dict)

	// Test Get za key1
	rec, err := Get(conf, []byte("key1"), dict, cbm)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if rec == nil || !bytes.Equal(rec.Value, []byte("value1")) {
		t.Errorf("expected value1, got %v", rec)
	}
	fmt.Print("Key1 pronađen: ", rec.Value, "\n")

	// Test Get za nepostojeći ključ
	rec, err = Get(conf, []byte("nokey"), dict, cbm)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if rec != nil {
		t.Errorf("expected nil for non-existent key, got %v", rec)
	}
	fmt.Print("Nepostojeći ključ vraća nil: ", rec, "\n")

	// Test kompakcije (size-tiered) ?
	err = Compact(conf, dict, cbm)
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}
	fmt.Print("Kompakcija uspešna\n")
}

func TestGetNextSSTableGeneration(t *testing.T) {
	conf := createTestConfig(t)

	levelDir := filepath.Join(conf.SSTable.SstableDirectory, "1")
	os.MkdirAll(levelDir, 0755)

	// Napravi foldere generacija 1, 2, 5
	os.MkdirAll(filepath.Join(levelDir, "1"), 0755)
	os.MkdirAll(filepath.Join(levelDir, "2"), 0755)
	os.MkdirAll(filepath.Join(levelDir, "5"), 0755)

	nextGen := GetNextSSTableGeneration(conf, 1)
	if nextGen != 6 {
		t.Errorf("expected next gen 6, got %d", nextGen)
	}

	// Test za nepostojeći direktorijum
	nextGen = GetNextSSTableGeneration(conf, 99)
	if nextGen != 1 {
		t.Errorf("expected next gen 1 for missing dir, got %d", nextGen)
	}
}

func TestCleanupNames(t *testing.T) {
	conf := createTestConfig(t)

	// Kreiraj refove sa "neurednim" generacijama
	refs := []*SSTableReference{
		{Level: 1, Gen: 3},
		{Level: 1, Gen: 5},
		{Level: 1, Gen: 2},
	}

	// Ručno kreiraj foldere i fajlove za njih (po potrebi)
	for _, ref := range refs {
		genDir := filepath.Join(conf.SSTable.SstableDirectory, "1", strconv.Itoa(ref.Gen))
		sstable.CreateDirectoryIfNotExists(genDir)
		file := filepath.Join(genDir, sstable.CreateFileName(genDir, ref.Gen, "SSTable", "db"))
		os.WriteFile(file, []byte("dummy"), 0644)
	}

	err := cleanupNames(conf, 1)
	if err != nil {
		t.Fatalf("cleanupNames failed: %v", err)
	}

	// Proveri da li su folderi/fajlovi preimenovani u 1,2,3
	for i := 1; i <= len(refs); i++ {
		genDir := filepath.Join(conf.SSTable.SstableDirectory, "1", strconv.Itoa(refs[i-1].Gen))
		if !sstable.FileExists(genDir) {
			t.Errorf("expected directory %s to exist", genDir)
		}
	}
}

func TestMergeTables(t *testing.T) {
	conf := createTestConfig(t)
	dict := compression.NewDictionary()

	dict.Add([]byte("a"))
	dict.Add([]byte("b"))

	// Kreiraj 2 SSTable sa po jednim zapisom
	ref1 := createTestSSTable(t, conf, 1, 1, []byte("a"), []byte("valueA"), dict)
	ref2 := createTestSSTable(t, conf, 1, 2, []byte("b"), []byte("valueB"), dict)

	err := mergeTables(conf, 2, cbm, dict, ref1, ref2)
	if err != nil {
		t.Fatalf("mergeTables failed: %v", err)
	}

	// Nakon merge, na nivou 2 treba postojati nova SSTable generacija 1
	newRefs, err := getSSTableReferences(conf, 2, true)
	if err != nil {
		t.Fatalf("failed to get SSTable references: %v", err)
	}

	found := false
	for _, ref := range newRefs {
		if ref.Gen == 1 {
			found = true
		}
	}

	if !found {
		t.Errorf("expected merged SSTable generation 1 at level 2")
	}
}
