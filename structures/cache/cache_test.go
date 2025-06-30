package cache

import (
	"testing"

	"github.com/iigor000/database/config"
)

// TestCache testira Put i Get funkcionalnosti kesiranja
// i izbacivanje najstarijeg elementa kad se dostigne kapacitet
func TestCache(t *testing.T) {
	cache := NewCache(&config.Config{Cache: config.CacheConfig{Capacity: 2}}) // Kapacitet keša je 2

	// Dodajemo dva elementa u keš
	cache.Put("a", []byte("value1"))
	cache.Put("b", []byte("value2"))

	// Proveravamo da li su elementi ispravno dodati
	if value, exists := cache.Get("a"); !exists || string(value) != "value1" {
		t.Errorf("Expected value1, got %s", string(value))
	}
	if value, exists := cache.Get("b"); !exists || string(value) != "value2" {
		t.Errorf("Expected value2, got %s", string(value))
	}

	cache.Put("c", []byte("value3")) // Ovo bi trebalo da izbaci key 1

	// Proveravamo da li je ključ 'a' izbačen i da li su ostali ispravni
	if _, exists := cache.Get("a"); exists {
		t.Errorf("Expected key 'a' to be evicted")
	}

	// Proveravamo da li su ostali elementi ispravni
	if value, exists := cache.Get("b"); !exists || string(value) != "value2" {
		t.Errorf("Expected value2, got %s", string(value))
	}
	if value, exists := cache.Get("c"); !exists || string(value) != "value3" {
		t.Errorf("Expected value3, got %s", string(value))
	}
}
