package cache

import (
	"testing"
)

// TestCache testira Put i Get funkcionalnosti kesiranja
// i izbacivanje najstarijeg elementa kad se dostigne kapacitet
func TestCache(t *testing.T) {
	cache := NewCache(2) // Kapacitet keša je 2

	// Dodajemo dva elementa u keš
	cache.Put(1, []byte("value1"))
	cache.Put(2, []byte("value2"))

	// Proveravamo da li su elementi ispravno dodati
	if value, exists := cache.Get(1); !exists || string(value) != "value1" {
		t.Errorf("Expected value1, got %s", string(value))
	}
	if value, exists := cache.Get(2); !exists || string(value) != "value2" {
		t.Errorf("Expected value2, got %s", string(value))
	}

	cache.Put(3, []byte("value3")) // Ovo bi trebalo da izbaci key 1

	// Proveravamo da li je key 1 izbačen i da li su ostali ispravni
	if _, exists := cache.Get(1); exists {
		t.Errorf("Expected key 1 to be evicted")
	}

	// Proveravamo da li su ostali elementi ispravni
	if value, exists := cache.Get(2); !exists || string(value) != "value2" {
		t.Errorf("Expected value2, got %s", string(value))
	}
	if value, exists := cache.Get(3); !exists || string(value) != "value3" {
		t.Errorf("Expected value3, got %s", string(value))
	}
}