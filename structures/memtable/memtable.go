package memtable

import (
	"fmt"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/structures/btree"
	"github.com/iigor000/database/structures/hashmap"
	"github.com/iigor000/database/structures/skiplist"
)

// Memtables struktura koja sadrzi vise Memtable-a
type Memtables struct {
	NumberOfMemtables int
	Memtables         map[int]*Memtable
	conf              *config.Config
	GenToFlush        int // Generacija za flush, koristi se za SSTable

}

// Konstruktor za Memtables strukturu
func NewMemtables(conf *config.Config) *Memtables {
	memtables := make(map[int]*Memtable)
	for i := 0; i < conf.Memtable.NumberOfMemtables; i++ {
		memtables[i] = NewMemtable(conf)
	}

	return &Memtables{
		NumberOfMemtables: conf.Memtable.NumberOfMemtables,
		Memtables:         memtables,
		conf:              conf,
		GenToFlush:        0, // Inicijalizujemo generaciju za flush na 0
	}
}

// CRUD operacije
// Update dodaje ili azurira na osnovu kljuca u Memtables
func (m *Memtables) Update(key []byte, value []byte, timestamp int64, tombstone bool) bool {
	// Prolazimo kroz sve Memtable i azuriramo

	flushed := false
	i := m.GetMemtableToChange()
	println(i)
	m.Memtables[i].Update(key, value, timestamp, tombstone)

	if i == m.NumberOfMemtables-1 {
		println("Last Memtable is being updated, checking if it needs to be flushed...")
		println("Memtable size:", m.Memtables[i].Size, "Capacity:", m.Memtables[i].Capacity)
		if m.Memtables[i].Size >= m.Memtables[i].Capacity {
			// Ako je poslednji Memtable pun, onda ga flush-ujemo na disk
			//m.memtables[0].FlushToDisk(m.conf, m.genToFlush)
			flushed = true

		}
	}

	return flushed
}

// Delete uklanja kljuc iz Memtables
func (m *Memtables) Delete(key []byte) bool {
	// Prolazimo kroz sve Memtable i brisemo
	for i := 0; i < m.NumberOfMemtables; i++ {
		memtable := m.Memtables[i]
		// Proveravamo da li postoji dati kljuc
		_, exist := memtable.Search(key)
		if exist {
			// Ako postoji, brisemo ga
			memtable.Delete(key)
			return true
		}
	}
	return false
}

// Search trazi kljuc u Memtables
func (m *Memtables) Search(key []byte) (*adapter.MemtableEntry, bool) {
	// Prolazimo kroz sve Memtable i trazimo
	for i := 0; i < m.NumberOfMemtables; i++ {
		memtable := m.Memtables[i]
		// Proveravamo da li postoji dati kljuc
		record, exist := memtable.Search(key)
		if exist {
			// Ako postoji, vracamo vrednost
			return record, true
		}
	}
	// Ako nismo nasli kljuc, vracamo false
	return nil, false
}

// Memtable struktura
type Memtable struct {
	Structure adapter.MemtableStructure
	Size      int
	Capacity  int
	Keys      [][]byte
}

// Konstruktor za Memtable strukturu, opcija za implementaciju skip listom ili binarnim stablom
func NewMemtable(conf *config.Config) *Memtable {
	var struc adapter.MemtableStructure
	switch conf.Memtable.Structure {
	case "skiplist":
		struc = skiplist.MakeSkipList(conf.Skiplist.MaxHeight)
	case "btree":
		struc = btree.NewBTree(16)
	case "hashmap":
		struc = hashmap.NewHashMap()
	default:
		struc = skiplist.MakeSkipList(conf.Skiplist.MaxHeight)
	}
	return &Memtable{Structure: struc, Size: 0, Capacity: conf.Memtable.NumberOfEntries}
}

// CRUD operacije
// Update dodaje ili azurira na osnovu kljuca u Memtable
func (m *Memtable) Update(key []byte, value []byte, timestamp int64, tombstone bool) {
	//println("Updating key:", string(key), "with value:", string(value), "timestamp:", timestamp, "tombstone:", tombstone)
	_, exist := m.Search(key)
	if !exist {
		m.Keys = append(m.Keys, key)
		m.Size++
	}
	m.Structure.Update(key, value, timestamp, tombstone)

}

func (m *Memtable) Delete(key []byte) {
	m.Structure.Delete(key)
}

func (m *Memtable) Search(key []byte) (*adapter.MemtableEntry, bool) {
	entry, found := m.Structure.Search(key)
	if !found {
		return nil, false
	}
	if entry.Tombstone {
		return entry, true
	}
	return entry, true
}

func (m *Memtable) Print() {
	for _, key := range m.Keys {
		entry, found := m.Structure.Search(key)
		if found {
			if !entry.Tombstone {
				fmt.Printf("Key: %s, Value: %s\n", key, entry.Value)
			}
		}
	}
}

func (m *Memtable) GetAllEntries() []adapter.MemtableEntry {
	entries := make([]adapter.MemtableEntry, 0, m.Size)
	for _, key := range m.Keys {
		entry, found := m.Structure.Search(key)
		if found && !entry.Tombstone {
			entries = append(entries, *entry)
		}
	}
	return entries
}

func (m *Memtables) GetMemtableToChange() int {

	for i := 0; i < m.NumberOfMemtables; i++ {
		memtable := m.Memtables[i]
		println(memtable.Capacity)

		if memtable.Size < memtable.Capacity {
			return i
		}
	}
	return -1
}
