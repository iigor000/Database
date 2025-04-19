package cache

import (
	"container/list"
	"sync"
)

type CacheItem struct {
	Key   int
	Value []byte
}

// Struktura koja predstavlja keš
type Cache struct {
	Capacity int                   // Kapacitet keš memorije
	Items    map[int]*list.Element // Heš mapa koja čuva ključeve i pokazivače na elemente u kešu
	List     *list.List            // Lista koja čuva elemente u redosledu pristupa
	Mu       sync.Mutex
}

// Funkcija za kreiranje novog keša
func NewCache(capacity int) *Cache {
	return &Cache{
		Capacity: capacity,
		Items:    make(map[int]*list.Element),
		List:     list.New(),
	}
}

// Funkcija za dobijanje vrednosti iz keša na osnovu ključa
func (c *Cache) Get(key int) ([]byte, bool) {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	if element, exists := c.Items[key]; exists {
		c.List.MoveToFront(element) // Pomeri element na početak liste
		return element.Value.(*CacheItem).Value, true
	}
	return nil, false // Ako ključ ne postoji, vrati null i false
}

func (c *Cache) Put(key int, value []byte) {
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
