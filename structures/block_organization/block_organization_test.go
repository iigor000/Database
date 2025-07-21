package block_organization

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/iigor000/database/config"
)

// TestBlockManagerWriteRead proverava da li BlockManager ispravno upisuje i cita blokove sa diska
func TestBlockManagerWriteRead(t *testing.T) {
	// Kreiramo privremeni direktorijum za test
	tempDir, err := os.MkdirTemp("", "block_manager_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Def konfiguracija
	cfg := &config.Config{
		Block: config.BlockConfig{
			BlockSize: 1024,
		},
		Cache: config.CacheConfig{
			Capacity: 10,
		},
	}
	bm := NewBlockManager(cfg)

	filePath := filepath.Join(tempDir, "testfile.dat")

	writeData := bytes.Repeat([]byte{0xAB}, cfg.Block.BlockSize)
	blockNumber := 0
	// Upisujemo blok
	if err := bm.WriteBlock(filePath, blockNumber, writeData); err != nil {
		t.Fatalf("WriteBlock failed: %v", err)
	}

	// itCamo blok
	readData, err := bm.ReadBlock(filePath, blockNumber)
	if err != nil {
		t.Fatalf("ReadBlock failed: %v", err)
	}

	// Poredjenje upisanih i procitanih podataka
	if !bytes.Equal(writeData, readData) {
		t.Errorf("Data mismatch: expected %v, got %v", writeData, readData)
	}
}

// TestBlockCache proverava osnovnu funkcionalnost kesiranja, dodavanja (put)
// i getovanje blokova kao i izbacivanje najstarijeg elementa kad se dostigne kapacitet
func TestBlockCache(t *testing.T) {
	// Konfiguracija sa malim kapacitetom za test (npr. kapacitet 2)
	cfg := &config.Config{
		Block: config.BlockConfig{
			BlockSize: 1024,
		},
		Cache: config.CacheConfig{
			Capacity: 2,
		},
	}
	bc := NewBlockCache(cfg)

	// Ubacujemo tri kljuca, a kapacitet je 2, pa se ocekuje da prvi ubaceni bude izbacen
	bc.Put("a", []byte("dataA"))
	bc.Put("b", []byte("dataB"))

	// Provera: oba "a" i "b" bi trebalo biti u kesu
	if data, ok := bc.Get("a"); !ok || !bytes.Equal(data, []byte("dataA")) {
		t.Errorf("Expected key 'a' to be in cache with value 'dataA'")
	}
	if data, ok := bc.Get("b"); !ok || !bytes.Equal(data, []byte("dataB")) {
		t.Errorf("Expected key 'b' to be in cache with value 'dataB'")
	}

	// Ubacujemo "c" sto bi trebalo da izbaci najstariji element "a"
	bc.Put("c", []byte("dataC"))

	// Kljucevi "b" i "c" treba da ostanu
	if _, ok := bc.Get("a"); ok {
		t.Errorf("Expected key 'a' to be evicted from cache")
	}
	if data, ok := bc.Get("b"); !ok || !bytes.Equal(data, []byte("dataB")) {
		t.Errorf("Expected key 'b' to remain in cache with value 'dataB'")
	}
	if data, ok := bc.Get("c"); !ok || !bytes.Equal(data, []byte("dataC")) {
		t.Errorf("Expected key 'c' to be in cache with value 'dataC'")
	}
}

// TestCachedBlockManager proverava da CachedBlockManager ispravno koristi kes prilikom citanja i pisanja
func TestCachedBlockManager(t *testing.T) {
	// Kreiramo privremeni direktorijum i fajl za test
	tempDir, err := os.MkdirTemp("", "cached_block_manager_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	filePath := filepath.Join(tempDir, "testfile.dat")

	// Konfiguracija za test
	cfg := &config.Config{
		Block: config.BlockConfig{
			BlockSize: 1024,
		},
		Cache: config.CacheConfig{
			Capacity: 10,
		},
	}
	// Kreiramo BlockManager i BlockCache
	bm := NewBlockManager(cfg)
	bc := NewBlockCache(cfg)
	cbm := &CachedBlockManager{
		BM: bm,
		C:  bc,
	}

	// Pripremamo podatke koje cemo upisati
	writeData := bytes.Repeat([]byte{0xCD}, cfg.Block.BlockSize)
	blockNumber := 0

	// Prvo, upisujemo blok preko CachedBlockManager-a
	if err := cbm.WriteBlock(filePath, blockNumber, writeData); err != nil {
		t.Fatalf("CachedBlockManager WriteBlock failed: %v", err)
	}

	// Citamo blok prvi put (treba da čita sa diska i stavlja u ked)
	readData1, err := cbm.ReadBlock(filePath, blockNumber)
	if err != nil {
		t.Fatalf("CachedBlockManager ReadBlock failed: %v", err)
	}
	if !bytes.Equal(writeData, readData1) {
		t.Errorf("Data mismatch on first read: expected %v, got %v", writeData, readData1)
	}

	// Modifikujemo fajl direktno (simuliramo promenu na disku)
	modifiedData := bytes.Repeat([]byte{0xEF}, cfg.Block.BlockSize)
	if err := bm.WriteBlock(filePath, blockNumber, modifiedData); err != nil {
		t.Fatalf("Direct BlockManager WriteBlock failed: %v", err)
	}

	// Citamo blok drugi put preko CachedBlockManager-a
	// Pošto je kesiran, oxekujemo da se vrati originalna vrednost (writeData), a ne modifiedData.
	readData2, err := cbm.ReadBlock(filePath, blockNumber)
	if err != nil {
		t.Fatalf("CachedBlockManager ReadBlock failed on second read: %v", err)
	}
	if !bytes.Equal(writeData, readData2) {
		t.Errorf("Cache did not return cached data: expected %v, got %v", writeData, readData2)
	}
}

func TestBlockManagerAppendBlock(t *testing.T) {
	// Kreiramo privremeni direktorijum za test
	tempDir, err := os.MkdirTemp("", "block_manager_append_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Def konfiguracija
	cfg := &config.Config{
		Block: config.BlockConfig{
			BlockSize: 1024,
		},
		Cache: config.CacheConfig{
			Capacity: 10,
		},
	}
	bm := NewBlockManager(cfg)

	filePath := filepath.Join(tempDir, "testfile.dat")

	writeData := bytes.Repeat([]byte{0xAB}, cfg.Block.BlockSize)
	// Upisujemo prvi blok
	blockNumber, err := bm.AppendBlock(filePath, writeData)
	if err != nil {
		t.Fatalf("AppendBlock failed: %v", err)
	}

	// Citamo prvi blok
	readData, err := bm.ReadBlock(filePath, blockNumber)
	if err != nil {
		t.Fatalf("ReadBlock failed: %v", err)
	}

	// Poredjenje upisanih i procitanih podataka
	if !bytes.Equal(writeData, readData) {
		t.Errorf("Data mismatch after append: expected %v, got %v", writeData, readData)
	}
}

// TestBlockManagerAppendMultiple proverava da li BlockManager ispravno dodaje blokove sa proverom
func TestBlockManagerAppend(t *testing.T) {
	// Kreiramo privremeni direktorijum za test
	tempDir, err := os.MkdirTemp("", "block_manager_append_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Def konfiguracija
	cfg := &config.Config{
		Block: config.BlockConfig{
			BlockSize: 1024,
		},
		Cache: config.CacheConfig{
			Capacity: 10,
		},
	}
	bm := NewBlockManager(cfg)

	filePath := filepath.Join(tempDir, "testfile.dat")

	writeData := bytes.Repeat([]byte{0xAB}, cfg.Block.BlockSize+64)
	// Upisujemo prvi blok
	blockNumber, err := bm.Append(filePath, writeData)
	if err != nil {
		t.Fatalf("AppendBlock failed: %v", err)
	}

	// Citamo prvi blok
	readData, err := bm.Read(filePath, blockNumber)
	if err != nil {
		t.Fatalf("ReadBlock failed: %v", err)
	}

	// Sklanjamo padding, sto bi radila struktura koja cita podatke
	readData = bytes.TrimRight(readData, "\x00")

	// Poredjenje upisanih i procitanih podataka
	if !bytes.Equal(writeData, readData) {
		t.Errorf("Data mismatch after append: expected %v, got %v", writeData, readData)
	}
}

func TestBlockManagerWrite(t *testing.T) {
	// Kreiramo privremeni direktorijum za test
	tempDir, err := os.MkdirTemp("", "block_manager_append_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Def konfiguracija
	cfg := &config.Config{
		Block: config.BlockConfig{
			BlockSize: 1024,
		},
		Cache: config.CacheConfig{
			Capacity: 10,
		},
	}
	bm := NewBlockManager(cfg)

	filePath := filepath.Join(tempDir, "testfile.dat")

	writeData := bytes.Repeat([]byte{0xAB}, cfg.Block.BlockSize+64)
	// Upisujemo prvi blok
	err = bm.Write(filePath, 0, writeData)
	if err != nil {
		t.Fatalf("AppendBlock failed: %v", err)
	}

	// Citamo prvi blok
	readData, err := bm.Read(filePath, 0)
	if err != nil {
		t.Fatalf("ReadBlock failed: %v", err)
	}

	// Sklanjamo padding, sto bi radila struktura koja cita podatke
	readData = bytes.TrimRight(readData, "\x00")

	// Poredjenje upisanih i procitanih podataka
	if !bytes.Equal(writeData, readData) {
		t.Errorf("Data mismatch after append: expected %v, got %v", writeData, readData)
	}
}
