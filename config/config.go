package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// Konfiguracijski fajl za blok organizaciju
type BlockConfig struct {
	BlockSize      int    `json:"block_size"`       // Moze biti 4/8/16 kb
	CacheCapacity  int    `json:"cache_capacity"`   // Kapacitet kes memorije
	WalSegmentSize int    `json:"wal_segment_size"` // Velicina segmenta u WAL-u
	Directory      string `json:"directory"`        // Direktorijum u kome se cuvaju blokovi

}

func LoadConfigFile(path string) (*BlockConfig, error) {
	defaultConfig := &BlockConfig{
		BlockSize:      4096,
		CacheCapacity:  100,
		WalSegmentSize: 100,
		Directory:      "data",
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

	switch defaultConfig.BlockSize {
	case 4096, 8192, 16384:
	default:
		return nil, errors.New("invalid block size - it must be value of 4096, 8192 or 16384")
	}
	return defaultConfig, nil
}
