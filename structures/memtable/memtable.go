package memtable

import (
	"bytes"
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
	m.Memtables[i].Update(key, value, timestamp, tombstone)

	if i == m.NumberOfMemtables-1 {
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

func (m *Memtables) GetEntryWithPrefix(prefix string, memtableIndex int, entryIndex int) (adapter.MemtableEntry, int, int) {
	// Prolazimo kroz sve Memtable i trazimo
	for i := memtableIndex; i < m.NumberOfMemtables; i++ {
		memtable := m.Memtables[i]
		// Proveravamo da li posmatrani kljuc ima prefiks
		for j := entryIndex; j < len(memtable.Keys); j++ {
			key := memtable.Keys[j]
			if bytes.HasPrefix(key, []byte(prefix)) {
				entry, found := memtable.Search(key)
				if found && !entry.Tombstone {
					return *entry, i, j // Vracamo entry, indeks Memtable-a i indeks kljuca
				}
			}
		}
	}
	return adapter.MemtableEntry{}, -1, -1 // Ako nismo nasli kljuc, vracamo nil i -1
}

func (m *Memtables) GetEntry(memtableIndex int, entryIndex int) (*adapter.MemtableEntry, bool) {
	if memtableIndex < 0 || memtableIndex >= len(m.Memtables) {
		return nil, false
	}
	memtable := m.Memtables[memtableIndex]
	if entryIndex < 0 || entryIndex >= len(memtable.Keys) {
		return nil, false
	}
	key := memtable.Keys[entryIndex]
	entry, found := memtable.Search(key)
	if !found {
		return nil, false
	}
	return entry, true
}

func (ms *Memtables) GetFirstEntry() adapter.MemtableEntry {
	if len(ms.Memtables) > 0 {
		for _, memtable := range ms.Memtables {
			if memtable.Size > 0 {
				firstKey := memtable.Keys[0]
				entry, _ := memtable.Search(firstKey)
				return *entry
			}
		}
	}
	return adapter.MemtableEntry{}
}

func (m *Memtable) GetFirstEntry() adapter.MemtableEntry {
	if m.Size > 0 {
		minKey := m.Keys[0]
		for _, key := range m.Keys {
			if bytes.Compare(key, minKey) < 0 {
				minKey = key
			}
		}
		entry, found := m.Search(minKey)
		if found {
			return *entry
		}
	}
	return adapter.MemtableEntry{}
}

func (m *Memtable) GetNextEntry(key []byte) (adapter.MemtableEntry, bool) {
	// Prolazimo kroz sve kljuceve i trazimo sledeci najmanji kljuc
	minKey := key
	for _, k := range m.Keys {
		if bytes.Compare(k, key) > 0 {
			if bytes.Equal(minKey, key) {
				minKey = k
			} else {
				if bytes.Compare(k, minKey) < 0 {
					minKey = k
				}
			}

		}
	}
	if bytes.Equal(minKey, key) {
		return adapter.MemtableEntry{}, false // Nema sledecih unosa
	}
	entry, found := m.Search(minKey)
	if found {
		return *entry, true
	}
	return adapter.MemtableEntry{}, false
}

func (m *Memtable) GetSize() int {
	return m.Size
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

		if memtable.Size < memtable.Capacity {
			return i
		}
	}
	return -1
}
