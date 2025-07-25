package hashmap

import (
	"time"

	"github.com/iigor000/database/structures/adapter"
)

type HashMap struct {
	data map[string]*adapter.MemtableEntry
}

func NewHashMap() *HashMap {
	return &HashMap{
		data: make(map[string]*adapter.MemtableEntry),
	}
}

func (h *HashMap) Search(key []byte) (*adapter.MemtableEntry, bool) {
	entry, found := h.data[string(key)]
	if !found {
		return &adapter.MemtableEntry{}, false
	}
	return entry, true
}

func (h *HashMap) Update(key []byte, value []byte, timestamp int64, tombstone bool) {
	h.data[string(key)] = &adapter.MemtableEntry{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
		Tombstone: tombstone,
	}
}

func (h *HashMap) Delete(key []byte) {
	record, found := h.Search(key)
	if !found {
		return
	}
	if record.Tombstone {
		return
	}
	record.Tombstone = true
	record.Value = nil // Clear the value
	record.Timestamp = int64(time.Now().Unix())
}

func (h *HashMap) Clear() {
	h.data = make(map[string]*adapter.MemtableEntry)
}
