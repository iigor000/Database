package lsmtree

import (
	"fmt"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/structures/block_organization"
)

// PrefixScanPaged vrši pretragu svih nivoa LSM stabla po prefiksu, uz paginaciju.
func PrefixScanPaged(conf *config.Config, prefix string, pageNumber int, pageSize int, cbm *block_organization.CachedBlockManager) ([]adapter.MemtableEntry, error) {
	allResults, err := PrefixScan(conf, prefix, cbm)
	if err != nil {
		return nil, err
	}
	if len(allResults) == 0 {
		return nil, fmt.Errorf("no records found for prefix: %s", prefix)
	}

	startIndex := pageNumber * pageSize
	if startIndex >= len(allResults) {
		return nil, fmt.Errorf("page %d out of bounds for prefix: %s", pageNumber, prefix)
	}
	endIndex := startIndex + pageSize
	if endIndex > len(allResults) {
		endIndex = len(allResults)
	}

	entries := make([]adapter.MemtableEntry, 0, endIndex-startIndex)
	for _, rec := range allResults[startIndex:endIndex] {
		entries = append(entries, adapter.MemtableEntry{
			Key:       rec.Key,
			Value:     rec.Value,
			Timestamp: rec.Timestamp,
			Tombstone: rec.Tombstone,
		})
	}

	return entries, nil
}

// RangeScanPaged vrši pretragu svih nivoa LSM stabla po opsegu ključeva, uz paginaciju.
func RangeScanPaged(conf *config.Config, startKey, endKey string, pageNumber int, pageSize int, cbm *block_organization.CachedBlockManager) ([]adapter.MemtableEntry, error) {
	allResults, err := RangeScan(conf, startKey, endKey, cbm)
	if err != nil {
		return nil, err
	}
	if len(allResults) == 0 {
		return nil, fmt.Errorf("no records found for range: %s - %s", startKey, endKey)
	}

	startIndex := pageNumber * pageSize
	if startIndex >= len(allResults) {
		return nil, fmt.Errorf("page %d out of bounds for range: %s - %s", pageNumber, startKey, endKey)
	}
	endIndex := startIndex + pageSize
	if endIndex > len(allResults) {
		endIndex = len(allResults)
	}

	entries := make([]adapter.MemtableEntry, 0, endIndex-startIndex)
	for _, rec := range allResults[startIndex:endIndex] {
		entries = append(entries, adapter.MemtableEntry{
			Key:       rec.Key,
			Value:     rec.Value,
			Timestamp: rec.Timestamp,
			Tombstone: rec.Tombstone,
		})
	}

	return entries, nil
}
