package hashmap

import (
	"time"

	"github.com/iigor000/database/structures/adapter"
)

type HashMap struct {
	data map[string]*adapter.MemtableEntry
}

func NewHashMap(capacity int) *HashMap {
	return &HashMap{
		data: make(map[string]*adapter.MemtableEntry),
	}
}

func (h *HashMap) Search(key string) (*adapter.MemtableEntry, bool) {
	entry, found := h.data[key]
	if !found {
		return &adapter.MemtableEntry{}, false
	}
	return entry, true
}

func (h *HashMap) Update(key string, value adapter.MemtableEntry) {
	h.data[key] = &value
}

func (h *HashMap) Delete(key string) {
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
