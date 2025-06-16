package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// Konfiguracijski fajl za blok organizaciju
type Config struct {
	Block    BlockConfig    `json:"block"`    // Konfiguracija bloka
	Wal      WalConfig      `json:"wal"`      // Konfiguracija WAL-a
	Memtable MemtableConfig `json:"memtable"` // Konfiguracija memtable-a
	Skiplist SkiplistConfig `json:"skiplist"` // Konfiguracija skip liste
	SSTable  SSTableConfig  `json:"sstable"`  // Konfiguracija SSTable-a
	Cache    CacheConfig    `json:"cache"`    // Konfiguracija kes memorije
}

type BlockConfig struct {
	BlockSize     int `json:"block_size"`     // Velicina bloka u bajtovima
	CacheCapacity int `json:"cache_capacity"` // Kapacitet kes memorije
}

type WalConfig struct {
	WalSegmentSize int    `json:"wal_segment_size"` // Velicina segmenta u WAL-u
	WalDirectory   string `json:"wal_directory"`    // Direktorijum u kome se cuvaju WAL segmenti
}

type MemtableConfig struct {
	NumberOfMemtables int    `json:"num"`         // Velicina memtable-a u bajtovima
	NumberOfEntries   int    `json:"num_entries"` // Broj unosa u memtable-u
	Structure         string `json:"struct"`      // Struktura memtable-a (npr. "skiplist", "tree")
}

type SkiplistConfig struct {
	MaxHeight int `json:"max_height"` // Maksimalna visina skip liste
}

type SSTableConfig struct {
	UseCompression   bool   `json:"use_compression"` // Kompresija SSTable-a true ili false
	IndexLevel       int    `json:"index_level"`     // Velicina index bloka u bajtovima
	SummaryLevel     int    `json:"summary_level"`   // Velicina filter bloka u bajtovima
	SstableDirectory string `json:"directory"`       // Direktorijum u kome se cuvaju SSTable-ovi
}

type CacheConfig struct {
	Capacity int `json:"capacity"` // Kapacitet kes memorije
}

func LoadConfigFile(path string) (*Config, error) {
	defaultConfig := &Config{
		Block: BlockConfig{
			BlockSize:     4096,
			CacheCapacity: 100,
		},
		Wal: WalConfig{
			WalSegmentSize: 100,
			WalDirectory:   "data",
		},
		Memtable: MemtableConfig{
			NumberOfMemtables: 10,
			NumberOfEntries:   1000,
			Structure:         "skiplist",
		},
		Skiplist: SkiplistConfig{
			MaxHeight: 16,
		},
		SSTable: SSTableConfig{
			UseCompression:   true,
			IndexLevel:       5,
			SummaryLevel:     10,
			SstableDirectory: "data/sstable",
		},
	}
	file, err := os.Open(path)
	if err != nil {
		return defaultConfig, nil
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(defaultConfig); err != nil {
		return nil, fmt.Errorf("error decoding config file: %v", err)
	}

	switch defaultConfig.Block.BlockSize {
	case 4096, 8192, 16384:
	default:
		return nil, errors.New("invalid block size - it must be value of 4096, 8192 or 16384")
	}
	return defaultConfig, nil
}
