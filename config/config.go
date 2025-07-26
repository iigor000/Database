package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// Konfiguracijski fajl za blok organizaciju
type Config struct {
	Block       BlockConfig       `json:"block"`        // Konfiguracija bloka
	Wal         WalConfig         `json:"wal"`          // Konfiguracija WAL-a
	Memtable    MemtableConfig    `json:"memtable"`     // Konfiguracija memtable-a
	Skiplist    SkiplistConfig    `json:"skiplist"`     // Konfiguracija skip liste
	BTree       BTreeConfig       `json:"btree"`        // Konfiguracija binarnog stabla
	SSTable     SSTableConfig     `json:"sstable"`      // Konfiguracija SSTable-a
	Cache       CacheConfig       `json:"cache"`        // Konfiguracija kes memorije
	LSMTree     LSMTreeConfig     `json:"lsmtree"`      // Konfiguracija LSM stabla
	TokenBucket TokenBucketConfig `json:"token_bucket"` // Konfiguracija token bucket-a
	Compression CompressionConfig `json:"compression"`  // Konfiguracija kompresije
}

type BlockConfig struct {
	BlockSize int `json:"block_size"` // Velicina bloka u bajtovima
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
	SummaryLevel     int    `json:"summary_level"`   // Velicina filter bloka u bajtovima
	SstableDirectory string `json:"directory"`       // Direktorijum u kome se cuvaju SSTable-ovi
	SingleFile       bool   `json:"single_file"`     // Da li se SSTable cuva u jednom fajlu ili u vise
}

type CacheConfig struct {
	Capacity int `json:"capacity"` // Kapacitet kes memorije
}

type TokenBucketConfig struct {
	StartTokens     int `json:"start_tokens"`      // Broj tokena na pocetku
	RefillIntervalS int `json:"refill_interval_s"` // Interval refilovanja tokena u sekundama
}

type LSMTreeConfig struct {
	MaxLevel            int    `json:"max_level"`            // Maksimalni nivo LSM stabla
	CompactionAlgorithm string `json:"compaction_algorithm"` // Algoritam za kompakciju (npr. "leveled", "size_tiered")
	// Leveled kompakcija - Svaki naredni može da bude N puta veći od prethodnog - granica BaseSSTableLimit * LevelSizeMultiplier^(Level)
	LevelSizeMultiplier int `json:"level_size_multiplier"` // Multiplikator velicine nivoa
	BaseSSTableLimit    int `json:"base_sstable_limit"`    // Bazni limit SSTable-a
	// Size_Tiered kompakcija - Kada se na nivou dostigne granicu od N SSTable-ova, vrši se kompakcija
	MaxTablesPerLevel int `json:"max_tables_per_level"` // Maksimalan broj SSTable-ova po nivou
}

type BTreeConfig struct {
	MinSize int `json:"min_size"` // Minimalna velicina binarnog stabla
}

type CompressionConfig struct {
	DictionaryDir string `json:"dictionary_dir"` // Direktorijum u kome se cuva recnik za kompresiju
}

func LoadConfigFile(path string) (*Config, error) {
	defaultConfig := &Config{
		Block: BlockConfig{
			BlockSize: 4096,
		},
		Wal: WalConfig{
			WalSegmentSize: 100,
			WalDirectory:   "data",
		},
		Memtable: MemtableConfig{
			NumberOfMemtables: 10,
			NumberOfEntries:   500,
			Structure:         "skiplist",
		},
		Skiplist: SkiplistConfig{
			MaxHeight: 16,
		},
		BTree: BTreeConfig{
			MinSize: 16,
		},
		SSTable: SSTableConfig{
			UseCompression:   true,
			SummaryLevel:     10,
			SstableDirectory: "data/sstable",
			SingleFile:       false,
		},
		Cache: CacheConfig{
			Capacity: 100,
		},
		LSMTree: LSMTreeConfig{
			MaxLevel:            5,
			CompactionAlgorithm: "size_tiered",
			// "leveled" KOMPAKCIJA
			BaseSSTableLimit:    10000, // Bazni limit SSTable-a (DataBlock je velicine 4096, 8192 ili 16384 bajta)
			LevelSizeMultiplier: 10,    // Multiplikator velicine nivoa (granica za prvi nivo je BaseSSTableLimit pomnožena sa 10, kod drugog sa 100, itd.)
			// "size_tiered" KOMPAKCIJA
			MaxTablesPerLevel: 8, // Maksimalan broj SSTable-ova po nivou
		},
		TokenBucket: TokenBucketConfig{
			StartTokens:     1000, // Broj tokena na pocetku
			RefillIntervalS: 120,  // Interval refilovanja tokena u sekundama
		},
		Compression: CompressionConfig{
			DictionaryDir: "data/compression_dict",
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

	switch defaultConfig.LSMTree.CompactionAlgorithm {
	case "size_tiered", "leveled":
	default:
		return nil, errors.New("invalid compaction algorithm - it must be 'size_tiered' or 'leveled'")
	}

	return defaultConfig, nil
}
