package cache

import (
	"testing"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/adapter"
)

// TestCache testira Put i Get funkcionalnosti kesiranja
// i izbacivanje najstarijeg elementa kad se dostigne kapacitet
func TestCacheBasicPutGet(t *testing.T) {
	cache := NewCache(&config.Config{Cache: config.CacheConfig{Capacity: 2}}) // Kapacitet keša je 2

	entry1 := adapter.MemtableEntry{Key: []byte("a"), Value: []byte("value1"), Timestamp: 0, Tombstone: false}
	entry2 := adapter.MemtableEntry{Key: []byte("b"), Value: []byte("value2"), Timestamp: 0, Tombstone: false}

	if err := cache.Put(entry1); err != nil {
		t.Fatalf("unexpected error inserting entry1: %v", err)
	}

	// Dodajemo dva elementa u keš
	if err := cache.Put(entry1); err != nil {
		t.Fatalf("unexpected error inserting entry1: %v", err)
	}
	if err := cache.Put(entry2); err != nil {
		t.Fatalf("unexpected error inserting entry2: %v", err)
	}

	// Proveravamo da li su elementi ispravno dodati
	if entry, exists := cache.Get("a"); !exists || string(entry.Value) != "value1" {
		t.Errorf("Expected value1, got %s", string(entry.Value))
	}
	if entry, exists := cache.Get("b"); !exists || string(entry.Value) != "value2" {
		t.Errorf("Expected value2, got %s", string(entry.Value))
	}
}

// TestCacheEviction testira izbacivanje najstarijeg elementa kada se dostigne kapacitet
func TestCacheEviction(t *testing.T) {
	cache := NewCache(&config.Config{Cache: config.CacheConfig{Capacity: 2}})

	cache.Put(adapter.MemtableEntry{Key: []byte("a"), Value: []byte("value1")})
	cache.Put(adapter.MemtableEntry{Key: []byte("b"), Value: []byte("value2")})

	// Pristupamo "a" da bismo ga označili kao skorije korišćen
	cache.Get("a")

	// Ubacujemo "c", što bi trebalo da izbaci "b"
	cache.Put(adapter.MemtableEntry{Key: []byte("c"), Value: []byte("value3")})

	if _, exists := cache.Get("b"); exists {
		t.Errorf("Očekivano je da ključ 'b' bude izbačen")
	}
	if _, exists := cache.Get("a"); !exists {
		t.Errorf("Očekivano je da ključ 'a' ostane u kešu")
	}
	if entry, exists := cache.Get("c"); !exists || string(entry.Value) != "value3" {
		t.Errorf("Očekivana vrednost value3, dobijeno: %v", entry)
	}
}

// TestCachePutEmptyKey testira ponašanje prilikom pokušaja ubacivanja unosa bez ključa
func TestCachePutEmptyKey(t *testing.T) {
	cache := NewCache(&config.Config{Cache: config.CacheConfig{Capacity: 1}})
	err := cache.Put(adapter.MemtableEntry{Key: []byte(""), Value: []byte("value")})
	if err == nil {
		t.Errorf("Očekivana greška pri ubacivanju praznog ključa, dobijeno: nil")
	}
}

// TestCacheTypeAssertionFailure testira neuspešan type assertion ako je element pokvaren
func TestCacheTypeAssertionFailure(t *testing.T) {
	cache := NewCache(&config.Config{Cache: config.CacheConfig{Capacity: 1}})

	// Ručno ubacujemo neispravan tip u mapu
	cache.Mu.Lock()
	cache.Items["invalid"] = cache.List.PushFront("nije entry") // Stavljamo string umesto *adapter.MemtableEntry
	cache.Mu.Unlock()

	// Pokušaj ažuriranja unosa sa istim ključem
	entry := adapter.MemtableEntry{Key: []byte("invalid"), Value: []byte("some")}
	err := cache.Put(entry)

	if err == nil || err.Error() != "cache: existing element type assertion failed" {
		t.Errorf("Očekivana greška prilikom type assertion-a, dobijeno: %v", err)
	}
}

// TestCacheEvictionTypeAssertionFailure testira grešku prilikom pokušaja izbacivanja neispravnog elementa
func TestCacheEvictionTypeAssertionFailure(t *testing.T) {
	cache := NewCache(&config.Config{Cache: config.CacheConfig{Capacity: 1}})

	// Popunjavamo keš validnim elementom
	cache.Put(adapter.MemtableEntry{Key: []byte("x"), Value: []byte("vx")})

	// Ručno kvarimo poslednji element
	cache.Mu.Lock()
	last := cache.List.Back()
	last.Value = "neispravan tip"
	cache.Mu.Unlock()

	// Pokrećemo izbacivanje
	err := cache.Put(adapter.MemtableEntry{Key: []byte("y"), Value: []byte("vy")})

	if err == nil || err.Error() != "cache: last element type assertion failed during eviction" {
		t.Errorf("Očekivana greška pri izbacivanju neispravnog tipa, dobijeno: %v", err)
	}
}
