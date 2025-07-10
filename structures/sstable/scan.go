package sstable

import "github.com/iigor000/database/structures/adapter"

func (s *SSTable) PrefixScan(prefix string, pageNumber int, pageSize int) ([]adapter.MemtableEntry, error) {

	entries := make([]adapter.MemtableEntry, 0)
	startIndex := pageNumber * pageSize
	endIndex := startIndex + pageSize

	for i, entry := range s.Data.Records {
		if i < startIndex {
			continue
		}
		if i >= endIndex {
			break
		}
		// TODO: Implementirati algoritam za trazenje prefiksa
		if len(entry.Key) >= len(prefix) && string(entry.Key[:len(prefix)]) == prefix {
			rec := adapter.MemtableEntry{
				Key:       entry.Key,
				Value:     entry.Value,
				Timestamp: entry.Timestamp,
				Tombstone: entry.Tombstone,
			}
			entries = append(entries, rec)
		}
	}

	return entries, nil
}

func (s *SSTable) RangeScan(minKey []byte, maxKey []byte, pageNumber int, pageSize int) ([]adapter.MemtableEntry, error) {
	entries := make([]adapter.MemtableEntry, 0)
	startIndex := pageNumber * pageSize
	endIndex := startIndex + pageSize

	//TODO: Implementirati algoritam za range scan
	for i, entry := range s.Data.Records {
		if i < startIndex {
			continue
		}
		if i >= endIndex {
			break
		}
		if len(entry.Key) >= len(minKey) && string(entry.Key[:len(minKey)]) >= string(minKey) &&
			len(entry.Key) <= len(maxKey) && string(entry.Key[:len(maxKey)]) <= string(maxKey) {
			rec := adapter.MemtableEntry{
				Key:       entry.Key,
				Value:     entry.Value,
				Timestamp: entry.Timestamp,
				Tombstone: entry.Tombstone,
			}
			entries = append(entries, rec)
		}
	}

	return entries, nil
}
