package cache

// TODO: Dodati error handling

import (
	"container/list"
	"sync"

	"github.com/iigor000/database/config"
)

type CacheItem struct {
	Key   string
	Value []byte
}

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
func (c *Cache) Get(key string) ([]byte, bool) {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	if element, exists := c.Items[key]; exists {
		c.List.MoveToFront(element) // Pomeri element na početak liste
		if element.Value != nil {
			return element.Value.(*CacheItem).Value, true
		}
	}
	return nil, false // Ako ključ ne postoji, vrati null i false
}

func (c *Cache) Put(key string, value []byte) {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	if element, exists := c.Items[key]; exists {
		c.List.MoveToFront(element)              // Pomeri postojeći element na početak
		element.Value.(*CacheItem).Value = value // Ažuriraj vrednost
		return
	}

	if len(c.Items) >= c.Capacity { // Ako je kapacitet keša pun, izbaci poslednji element
		lastElement := c.List.Back()
		if lastElement != nil {
			delete(c.Items, lastElement.Value.(*CacheItem).Key) // Ukloni najstariji element iz heš mape
			c.List.Remove(lastElement)                          // Ukloni ga iz liste
		}
	}

	newItem := &CacheItem{Key: key, Value: value}
	element := c.List.PushFront(newItem) // Dodaj novi element na početak liste
	c.Items[key] = element               // Dodaj ga u heš mapu
}
