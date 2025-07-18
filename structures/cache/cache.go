package cache

import (
	"container/list"
	"errors"
	"sync"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/adapter"
)

// Struktura koja predstavlja keš
type Cache struct {
	Capacity int                      // Kapacitet keš memorije
	Items    map[string]*list.Element // Heš mapa koja čuva ključeve i pokazivače na elemente u kešu
	List     *list.List               // Lista koja čuva elemente u redosledu pristupa
	Mu       sync.Mutex
}

// Funkcija za kreiranje novog keša
func NewCache(config *config.Config) *Cache {
	return &Cache{
		Capacity: config.Cache.Capacity,
		Items:    make(map[string]*list.Element),
		List:     list.New(),
	}
}

// Funkcija za dobijanje vrednosti iz keša na osnovu ključa
func (c *Cache) Get(key string) (*adapter.MemtableEntry, bool) {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	if element, exists := c.Items[key]; exists {
		c.List.MoveToFront(element) // Pomeri element na početak liste
		if element.Value != nil {
			if entry, ok := element.Value.(*adapter.MemtableEntry); ok {
				return entry, true
			}
		}
	}
	return nil, false // Ako ključ ne postoji, vrati null i false
}

func (c *Cache) Put(entry adapter.MemtableEntry) error {
	if len(entry.Key) == 0 {
		return errors.New("cache: entry key is empty")
	}

	c.Mu.Lock()
	defer c.Mu.Unlock()

	keyStr := string(entry.Key)

	if element, exists := c.Items[keyStr]; exists {
		c.List.MoveToFront(element) // Pomeri postojeći element na početak
		if existingEntry, ok := element.Value.(*adapter.MemtableEntry); ok {
			existingEntry.Value = entry.Value         // Ažuriraj vrednost
			existingEntry.Timestamp = entry.Timestamp // Ažuriraj i timestamp
			existingEntry.Tombstone = entry.Tombstone // Ažuriraj i tombstone
			return nil
		} else {
			return errors.New("cache: existing element type assertion failed")
		}
	}

	if len(c.Items) >= c.Capacity { // Ako je kapacitet keša pun, izbaci poslednji element
		lastElement := c.List.Back()
		if lastElement != nil {
			if lastEntry, ok := lastElement.Value.(*adapter.MemtableEntry); ok {
				delete(c.Items, string(lastEntry.Key)) // Ukloni najstariji element iz heš mape
			} else {
				return errors.New("cache: last element type assertion failed during eviction")
			}
			c.List.Remove(lastElement) // Ukloni ga iz liste
		}
	}

	// Store a pointer to the entry, not the entry itself
	entryPtr := &entry
	element := c.List.PushFront(entryPtr) // Dodaj novi element na početak liste
	c.Items[keyStr] = element             // Dodaj ga u heš mapu

	return nil
}
