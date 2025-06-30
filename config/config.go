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
	LSMTree  LSMTreeConfig  `json:"lsmtree"`  // Konfiguracija LSM stabla
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
	SummaryLevel     int    `json:"summary_level"`   // Velicina filter bloka u bajtovima
	SstableDirectory string `json:"directory"`       // Direktorijum u kome se cuvaju SSTable-ovi
}

type CacheConfig struct {
	Capacity int `json:"capacity"` // Kapacitet kes memorije
}

type LSMTreeConfig struct {
	MaxLevel               int    `json:"max_level"`                 // Maksimalni nivo LSM stabla
	CompactionAlgorithm    string `json:"compaction_algorithm"`      // Algoritam za kompakciju (npr. "size_tiered", "leveled")
	UseSizeBasedCompaction bool   `json:"use_size_based_compaction"` // Koristi size-based kompakciju (poredi po MB) ili ne (poredi po broju SSTable-ova)
	BaseLevelSizeMBLimit   int    `json:"max_level_size_mb"`         // (koristi se AKO je UseSizeBasedCompaction true i algoritam za kompakciju "leveled") Maksimalna velicina nivoa u MB
	BaseSSTableLimit       int    `json:"base_sstable_limit"`        // (koristi se AKO je UseSizeBasedCompaction false i algoritam za kompakciju "leveled") Granica za bazni SSTable u MB
	LevelSizeMultiplier    int    `json:"level_size_multiplier"`     // (koristi se kod "leveled" kompakcije) Multiplikator velicine nivoa
	MaxSSTablesPerLevel    []int  `json:"max_sstables_per_level"`    // (koristi se AKO je algoritam za kompakciju "size_tiered") Maksimalan broj SSTable-ova po nivou
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
			SummaryLevel:     10,
			SstableDirectory: "data/sstable",
		},
		Cache: CacheConfig{
			Capacity: 100,
		},
		LSMTree: LSMTreeConfig{
			MaxLevel:               5,
			CompactionAlgorithm:    "size_tiered",
			UseSizeBasedCompaction: true,
			// "LEVELED" KOMPAKCIJA
			BaseLevelSizeMBLimit: 100, // Maksimalna velicina nivoa u MB (koristi se AKO je UseSizeBasedCompaction true)
			BaseSSTableLimit:     4,   // Granica za bazni SSTable u MB (koristi se AKO je UseSizeBasedCompaction false)
			LevelSizeMultiplier:  10,  // Multiplikator velicine nivoa
			// "SIZE-TIERED" KOMPAKCIJA
			MaxSSTablesPerLevel: []int{4, 8, 16, 32, 64}, // Maksimalan broj SSTable-ova po nivou
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

	if defaultConfig.LSMTree.CompactionAlgorithm == "leveled" {
		if defaultConfig.LSMTree.UseSizeBasedCompaction {
			if defaultConfig.LSMTree.BaseLevelSizeMBLimit <= 0 {
				return nil, errors.New("invalid base level size MB limit - it must be greater than 0")
			}
			if defaultConfig.LSMTree.BaseSSTableLimit <= 0 {
				return nil, errors.New("invalid base SSTable limit - it must be greater than 0")
			}
			if defaultConfig.LSMTree.LevelSizeMultiplier <= 0 {
				return nil, errors.New("invalid level size multiplier - it must be greater than 0")
			}
		} else {
			if len(defaultConfig.LSMTree.MaxSSTablesPerLevel) == 0 {
				return nil, errors.New("max SSTables per level must be defined for size_tiered compaction")
			}
			for _, maxSSTables := range defaultConfig.LSMTree.MaxSSTablesPerLevel {
				if maxSSTables <= 0 {
					return nil, errors.New("invalid max SSTables per level - it must be greater than 0")
				}
			}
		}
	} else if len(defaultConfig.LSMTree.MaxSSTablesPerLevel) != defaultConfig.LSMTree.MaxLevel {
		return nil, errors.New("the length of the list of 'MaxSSTablesPerLevel' and the length of LSMTree - 'MaxLevel' must be the same")
	}

	return defaultConfig, nil
}
