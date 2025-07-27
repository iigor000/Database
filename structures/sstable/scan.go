package sstable

import (
	"fmt"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/structures/block_organization"
)

func (s *SSTable) PrefixScan(prefix string, pageNumber int, pageSize int, conf *config.Config, bm *block_organization.CachedBlockManager) ([]adapter.MemtableEntry, error) {

	entries := make([]adapter.MemtableEntry, 0)
	startIndex := pageNumber * pageSize
	endIndex := startIndex + pageSize
	prefixIter := s.PrefixIterate(prefix, bm)
	if prefixIter == nil {
		return nil, fmt.Errorf("failed to create Prefix iterator for prefix: %s", prefix)
	}
	for i := 0; i < startIndex; i++ {
		_, ok := prefixIter.Next()
		if !ok {
			break
		}
	}
	for i := startIndex; i < endIndex; i++ {
		entry, ok := prefixIter.Next()
		if !ok {
			break
		}
		entries = append(entries, entry)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("no records found for prefix: %s", prefix)
	}
	if len(entries) < endIndex-startIndex {
		println("Warning: fewer records found than requested in PrefixScan")
	}

	return entries, nil
}

func (s *SSTable) RangeScan(minKey []byte, maxKey []byte, pageNumber int, pageSize int, conf *config.Config, bm *block_organization.CachedBlockManager) ([]adapter.MemtableEntry, error) {
	entries := make([]adapter.MemtableEntry, 0)
	startIndex := pageNumber * pageSize
	endIndex := startIndex + pageSize
	rangeIter := s.RangeIterate(string(minKey), string(maxKey), bm)
	if rangeIter == nil {
		return nil, fmt.Errorf("failed to create Range iterator for range: %s - %s", string(minKey), string(maxKey))
	}
	for i := 0; i < startIndex; i++ {
		_, ok := rangeIter.Next()
		if !ok {
			break
		}
	}
	for i := startIndex; i < endIndex; i++ {
		entry, ok := rangeIter.Next()
		if !ok {
			break
		}
		entries = append(entries, entry)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("no records found for range: %s - %s", string(minKey), string(maxKey))
	}
	if len(entries) < endIndex-startIndex {
		println("Warning: fewer records found than requested in RangeScan")
	}

	return entries, nil
}
