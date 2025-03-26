package block_organization

import (
	"container/list"
	"sync"

	"github.com/iigor000/database/config"
)

// Blok kes struktura
type BlockCache struct {
	capacity  int
	cache     map[string]*list.Element // Mapa koja cuva kljuc i pokazivac na elemente u dvostruko spregnutoj listi
	list      *list.List               // Dvostruko spregnuta lista koja cuva blokove podataka u redosledu pristupa
	blockSize int
	mu        sync.Mutex // Sprecava istovremeni modifikacije kesa
}

// Struktura koja cuva kljuc i blok podataka
type cacheData struct {
	key   string // SLuzi za identifikaciju bloka
	block []byte
}

func NewBlockCache(cfg config.BlockConfig) *BlockCache {
	return &BlockCache{
		capacity:  cfg.CacheCapacity,
		cache:     make(map[string]*list.Element),
		list:      list.New(),
		blockSize: cfg.BlockSize,
	}
}

// Funkcija, na osnovu kljuca, dobavlja blok podataka iz kesa, ako postoji pomeramo ga na pocetak liste i returnujemo blok taj
func (bc *BlockCache) Get(key string) ([]byte, bool) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	if element, isThere := bc.cache[key]; isThere { // Proveravamo da li kljuc postoji u mapi (cache)
		// elem predstavlja pokazivac na element u listi
		bc.list.MoveToFront(element)
		return element.Value.(*cacheData).block, true
	}
	return nil, false // Ako kljuc ne postoji u mapi (cache) vracamo ove povratne vrednosti
}

// Funkcija koja dodaje blok u kes
func (bc *BlockCache) Put(key string, block []byte) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	if element, isThere := bc.cache[key]; isThere {
		bc.list.MoveToFront(element)
		element.Value.(*cacheData).block = block // Menjamo vrednost bloka
		return
	}
	if len(bc.cache) >= bc.capacity { // Ako je kapacitet kesa pun, izbacujemo poslednji element
		lastElement := bc.list.Back()
		delete(bc.cache, lastElement.Value.(*cacheData).key) // Uklnjamo najstariji blok iz mape i iz liste
		bc.list.Remove(lastElement)
	}
	element := bc.list.PushFront(&cacheData{key: key, block: block}) // Dodajemo novi element na pocetak liste
	bc.cache[key] = element                                          // Dodajemo novi element u mapu (cache)
}
