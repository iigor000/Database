package cache

import (
	"testing"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/adapter"
)

// TestCache testira Put i Get funkcionalnosti kesiranja
// i izbacivanje najstarijeg elementa kad se dostigne kapacitet
func TestCache(t *testing.T) {
	cache := NewCache(&config.Config{Cache: config.CacheConfig{Capacity: 2}}) // Kapacitet keša je 2

	entry1 := adapter.MemtableEntry{Key: []byte("a"), Value: []byte("value1"), Timestamp: 0, Tombstone: false}
	entry2 := adapter.MemtableEntry{Key: []byte("b"), Value: []byte("value2"), Timestamp: 0, Tombstone: false}

	// Dodajemo dva elementa u keš
	cache.Put(entry1)
	cache.Put(entry2)

	// Proveravamo da li su elementi ispravno dodati
	if entry, exists := cache.Get("a"); !exists || string(entry.Value) != "value1" {
		t.Errorf("Expected value1, got %s", string(entry.Value))
	}
	if entry, exists := cache.Get("b"); !exists || string(entry.Value) != "value2" {
		t.Errorf("Expected value2, got %s", string(entry.Value))
	}

	entry3 := adapter.MemtableEntry{Key: []byte("c"), Value: []byte("value3"), Timestamp: 0, Tombstone: false}

	cache.Put(entry3) // Ovo bi trebalo da izbaci key 1

	// Proveravamo da li je ključ 'a' izbačen i da li su ostali ispravni
	if _, exists := cache.Get("a"); exists {
		t.Errorf("Expected key 'a' to be evicted")
	}

	// Proveravamo da li su ostali elementi ispravni
	if entry, exists := cache.Get("b"); !exists || string(entry.Value) != "value2" {
		t.Errorf("Expected value2, got %s", string(entry.Value))
	}
	if entry, exists := cache.Get("c"); !exists || string(entry.Value) != "value3" {
		t.Errorf("Expected value3, got %s", string(entry.Value))
	}
}
