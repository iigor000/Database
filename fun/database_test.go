package fun

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/iigor000/database/config"
)

func createTestDatabase(t *testing.T) (*Database, func()) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "db_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	cfg, err := config.LoadConfigFile("../config/config.json")
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to load config: %v", err)
	}

	// Override paths
	cfg.Wal.WalDirectory = filepath.Join(tempDir, "wal")
	cfg.SSTable.SstableDirectory = filepath.Join(tempDir, "sstable")
	cfg.TokenBucket.StartTokens = 100 // Enough for all test operations
	cfg.TokenBucket.RefillIntervalS = 1

	// Create required directories
	dirs := []string{
		cfg.Wal.WalDirectory,
		cfg.SSTable.SstableDirectory,
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			os.RemoveAll(tempDir)
			t.Fatalf("Failed to create directory %s: %v", dir, err)
		}
	}

	db, err := NewDatabase(cfg, "testuser")
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create database: %v", err)
	}

	// CREATE TOKEN BUCKET FOR TEST USER
	if err := CreateBucket(db); err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create token bucket: %v", err)
	}

	return db, func() {
		os.RemoveAll(tempDir)
	}
}

func TestMakeDatabase(t *testing.T) {

	db, cleanup := createTestDatabase(t)
	defer cleanup()

	if db.username != "testuser" {
		t.Errorf("Expected username 'testuser', got %s", db.username)
	}
}

func TestDatabase_PutGet(t *testing.T) {
	db, cleanup := createTestDatabase(t)
	defer cleanup()

	key := "testkey"
	value := []byte("testvalue")

	err := db.Put(key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	storedValue, found, err := db.Get(key) // Use exported Get
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Error("Key not found after put")
	}
	if string(storedValue) != string(value) {
		t.Errorf("Value mismatch, expected %q got %q", value, storedValue)
	}
}

func TestDatabase_Delete(t *testing.T) {
	db, cleanup := createTestDatabase(t)
	defer cleanup()

	key := "testkey"
	value := []byte("testvalue")

	// First put a value
	err := db.Put(key, value)
	if err != nil {
		t.Fatalf("Initial put failed: %v", err)
	}

	// Then delete it
	err = db.Delete(key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deletion
	_, found, err := db.Get(key) // Use exported Get
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}
	if found {
		t.Error("Key still found after delete")
	}
}

func TestDatabase_PutMany(t *testing.T) {
	// db, cleanup := createTestDatabase(t)
	// defer cleanup()
	// config, err := config.LoadConfigFile("../config/config.json")
	// if err != nil {
	// 	panic(err)
	// }
	// db, err := NewDatabase(config, "root")
	// if err != nil {
	// 	panic(err)
	// }

	dataDir := fmt.Sprintf("testdata/%s", "db_test")
	fmt.Printf("Korišćenje postojećeg direktorijuma: %s\n", dataDir)
	defer os.RemoveAll(dataDir)
	// Konfiguracija za test
	cfg := &config.Config{
		Block: config.BlockConfig{
			BlockSize: 4096,
		},
		Wal: config.WalConfig{
			WalSegmentSize: 1024 * 1024,
			WalDirectory:   filepath.Join(dataDir, "wal"),
		},
		Memtable: config.MemtableConfig{
			NumberOfMemtables: 2,
			NumberOfEntries:   40,
			Structure:         "skiplist",
		},
		Skiplist: config.SkiplistConfig{
			MaxHeight: 12,
		},
		SSTable: config.SSTableConfig{
			UseCompression:   false,
			SummaryLevel:     10,
			SstableDirectory: "data/sstable",
			SingleFile:       false,
		},
		Cache: config.CacheConfig{
			Capacity: 1000,
		},
		TokenBucket: config.TokenBucketConfig{
			StartTokens:     10000,
			RefillIntervalS: 1,
		},
		LSMTree: config.LSMTreeConfig{
			MaxLevel:            7,
			CompactionAlgorithm: "size_tiered",
			LevelSizeMultiplier: 10,
			BaseSSTableLimit:    10000,
			MaxTablesPerLevel:   10,
		},
		BTree: config.BTreeConfig{
			MinSize: 16,
		},
		Compression: config.CompressionConfig{
			DictionaryDir: filepath.Join(dataDir, "compression.db"),
		},
	}

	// Kreirajte samo osnovne direktorijume - NE kreirajte fajlove kao direktorijume
	requiredDirs := []string{
		cfg.Wal.WalDirectory,
		cfg.SSTable.SstableDirectory,
	}

	// Kreirajte SAMO direktorijume za nivoe, ne i fajlove unutar njih
	for level := 0; level <= cfg.LSMTree.MaxLevel; level++ {
		levelDir := filepath.Join(cfg.SSTable.SstableDirectory, strconv.Itoa(level))
		requiredDirs = append(requiredDirs, levelDir)
	}

	// Kreiraj direktorijume
	for _, dir := range requiredDirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("Ne mogu da kreiram direktorijum %s: %v", dir, err)
		}
	}

	// Inicijalizacija baze
	fmt.Println("Inicijalizacija baze...")
	db, err := NewDatabase(cfg, "testuser")
	if err != nil {
		t.Fatalf("Neuspešna inicijalizacija baze: %v", err)
	}
	fmt.Println("Baza uspešno inicijalizovana")

	// Kreiranje token bucketa
	fmt.Println("Kreiranje token bucketa za korisnika...")
	if err := CreateBucket(db); err != nil {
		t.Fatalf("Neuspešno kreiranje token bucketa: %v", err)
	}
	fmt.Println("Token bucket uspešno kreiran")

	// Priprema test podataka
	const testCount = 100
	entries := make(map[string][]byte, testCount)
	fmt.Printf("Priprema %d testnih unosa...\n", testCount)
	for i := 0; i < testCount; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		entries[key] = []byte(value)
	}

	// Ubacivanje podataka
	fmt.Println("Početak ubacivanja podataka...")
	for k, v := range entries {
		fmt.Printf("Ubacivanje ključa: %s\n", k)
		if err := db.Put(k, v); err != nil {
			t.Fatalf("Greška pri ubacivanju ključa %s: %v", k, err)
		}
	}

	fmt.Println("Svi podaci uspešno ubaceni")

	// Provera podataka
	fmt.Println("Početak provere podataka...")
	for k, v := range entries {
		fmt.Printf("Provera ključa: %s\n", k)
		value, _, err := db.Get(k)
		if err != nil {
			t.Fatalf("Greška pri dohvatanju ključa %s: %v", k, err)
		}
		if string(value) != string(v) {
			t.Errorf("Vrednost za ključ %s se ne poklapa, očekivano: %s, dobijeno: %s", k, v, value)
		}

	}
	fmt.Println("Svi podaci uspešno provereni")
}
