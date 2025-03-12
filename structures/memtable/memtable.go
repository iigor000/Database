package memtable

import (
	"fmt"
	"time"

	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/structures/skiplist"
)

// Memtable struktura
type Memtable struct {
	structure adapter.MemtableStructure
	size      int
	capacity  int
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
func (m *Memtable) Create(key int, value []byte) {
	if m.size >= m.capacity {
		m.FlushToDisk()
		m.size = 0
	}
	m.structure.Create(key, value, time.Now().UnixNano(), false)
	m.size++
}

func (m *Memtable) Update(key int, value []byte) {
	m.structure.Create(key, value, time.Now().UnixNano(), false)
}

func (m *Memtable) Delete(key int) {
	m.structure.Delete(key)
}

func (m *Memtable) Read(key int) ([]byte, bool) {
	entry, found := m.structure.Read(key)
	if !found || entry.Tombstone {
		return nil, false
	}
	return entry.Value, true
}

func (m *Memtable) Print() {
	for i := 0; i < 100; i++ {
		entry, found := m.structure.Read(i)
		if found {
			println(i, string(entry.Value))
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
