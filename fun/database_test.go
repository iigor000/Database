package fun

import (
	"fmt"
	"os"
	"path/filepath"
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
	cfg.TokenBucket.StartTokens = 1000 // Enough for all test operations
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

// TODO: FIX Ne radi kada je useCompression true (potrebno implementirati kompresiju kod LSMTree)
// TODO: FIX Ne radi kada je singleFile true (neophodno implementirati SingleFile pristup kod Get() u SSTable)
func TestDatabase_PutMany(t *testing.T) {
	// db, cleanup := createTestDatabase(t)
	// defer cleanup()
	config, err := config.LoadConfigFile("../config/config.json")
	if err != nil {
		panic(err)
	}
	db, err := NewDatabase(config, "root")
	if err != nil {
		panic(err)
	}

	// Create test data
	const testCount = 100
	entries := make(map[string][]byte, testCount)
	for i := 0; i < testCount; i++ {
		entries[fmt.Sprintf("key%d", i)] = []byte(fmt.Sprintf("value%d", i))
	}

	// Insert all entries
	for k, v := range entries {
		if err := db.Put(k, v); err != nil {
			t.Fatalf("Put failed for key %s: %v", k, err)
		}
	}
	//fmt.Println("############################################################################################################")
	// Verify all entries
	println("Verifying entries...")
	for k, v := range entries {
		storedValue, found, err := db.Get(k) // Use exported Get
		if err != nil {
			t.Fatalf("Get failed for key %s: %v", k, err)
		}

		if string(storedValue) != string(v) {
			t.Errorf("Value mismatch for key %s, expected %q got %q", k, v, storedValue)
		}
		println("Key:", k, "Value:", string(storedValue), "Found:", found)
	}
}
