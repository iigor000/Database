package config

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigFile_DefaultConfigOnMissingFile(t *testing.T) {
	tmpDir := t.TempDir()
	missingFile := filepath.Join(tmpDir, "nonexistent.json")

	cfg, err := LoadConfigFile(missingFile)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if cfg.Block.BlockSize != 4096 {
		t.Errorf("expected default BlockSize 4096, got %d", cfg.Block.BlockSize)
	}
	if cfg.Wal.WalDirectory != "data" {
		t.Errorf("expected default WalDirectory 'data', got %s", cfg.Wal.WalDirectory)
	}
}

func TestLoadConfigFile_LoadsValidConfig(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "config-*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	configJSON := `{
		"block": {"block_size": 8192, "cache_capacity": 200},
		"wal": {"wal_segment_size": 200, "wal_directory": "wal_dir"},
		"memtable": {"num": 5, "num_entries": 500, "struct": "tree"},
		"skiplist": {"max_height": 8}
	}`
	if _, err := tmpFile.Write([]byte(configJSON)); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}
	tmpFile.Close()

	cfg, err := LoadConfigFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if cfg.Block.BlockSize != 8192 {
		t.Errorf("expected BlockSize 8192, got %d", cfg.Block.BlockSize)
	}
	if cfg.Wal.WalDirectory != "wal_dir" {
		t.Errorf("expected WalDirectory 'wal_dir', got %s", cfg.Wal.WalDirectory)
	}
	if cfg.Memtable.Structure != "tree" {
		t.Errorf("expected Memtable Structure 'tree', got %s", cfg.Memtable.Structure)
	}
	if cfg.Skiplist.MaxHeight != 8 {
		t.Errorf("expected Skiplist MaxHeight 8, got %d", cfg.Skiplist.MaxHeight)
	}
}

func TestLoadConfigFile_InvalidBlockSize(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "config-*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	configJSON := `{
		"block": {"block_size": 1234, "cache_capacity": 200},
		"wal": {"wal_segment_size": 200, "wal_directory": "wal_dir"},
		"memtable": {"num": 5, "num_entries": 500, "struct": "tree"},
		"skiplist": {"max_height": 8}
	}`
	if _, err := tmpFile.Write([]byte(configJSON)); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}
	tmpFile.Close()

	_, err = LoadConfigFile(tmpFile.Name())
	if err == nil {
		t.Fatalf("expected error for invalid block size, got nil")
	}
}

func TestLoadConfigFile_InvalidJSON(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "config-*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	invalidJSON := `{"block": {"block_size": 4096, "cache_capacity": 100},`
	if _, err := tmpFile.Write([]byte(invalidJSON)); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}
	tmpFile.Close()

	_, err = LoadConfigFile(tmpFile.Name())
	if err == nil {
		t.Fatalf("expected error for invalid JSON, got nil")
	}
}
