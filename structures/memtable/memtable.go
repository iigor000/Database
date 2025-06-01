package memtable

import (
	"fmt"

	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/structures/skiplist"
)

// Memtable struktura
type Memtable struct {
	structure adapter.MemtableStructure
	size      int
	capacity  int
	keys      [][]byte
}

// Konstruktor za Memtable strukturu, opcija za implementaciju skip listom ili binarnim stablom
func NewMemtable(useSkipList bool, maxHeight int, n int) *Memtable {
	var struc adapter.MemtableStructure
	if useSkipList {
		struc = skiplist.MakeSkipList(maxHeight)
	} else {
		//a = bst.NewBST()
	}
	return &Memtable{structure: struc, size: 0, capacity: n}
}

// CRUD operacije
// Update dodaje ili azurira na osnovu kljuca u Memtable
func (m *Memtable) Update(key []byte, value []byte, timestamp int64, tombstone bool) {
	_, exist := m.Search(key)
	if !exist {
		m.keys = append(m.keys, key)
	}
	m.structure.Update(key, value, timestamp, tombstone)

}

func (m *Memtable) Delete(key []byte) {
	m.structure.Delete(key)
}

func (m *Memtable) Search(key []byte) ([]byte, bool) {
	entry, found := m.structure.Search(key)
	if !found || entry.Tombstone {
		return nil, false
	}
	return entry.Value, true
}

func (m *Memtable) Print() {
	for _, key := range m.keys {
		entry, found := m.structure.Search(key)
		if found {
			if !entry.Tombstone {
				fmt.Printf("Key: %s, Value: %s\n", key, entry.Value)
			}
		}
	}
}

func (m *Memtable) FlushToDisk() {
	// Simulacija flush-a na disk
	fmt.Println("Flushing Memtable to disk...")
	m.Print()
	// Ovde bi se podaci upisivali na disk (u SSTable)
	m.structure.Clear()
	//m.structure = skiplist.MakeSkipList(m.maxHeight)
	m.Print()
}
