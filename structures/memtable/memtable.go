package memtable

import (
	"fmt"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/structures/skiplist"
)

// Memtables struktura koja sadrzi vise Memtable-a
type Memtables struct {
	numberOfMemtables int
	memtables         map[int]*Memtable
	conf              config.Config
}

// Konstruktor za Memtables strukturu
func NewMemtables(conf config.Config) *Memtables {
	memtables := make(map[int]*Memtable)
	if conf.Memtable.Structure == "skiplist" {
		for i := 0; i < conf.Memtable.NumberOfMemtables; i++ {
			memtables[i] = NewMemtable(true, conf.Skiplist.MaxHeight, conf.Memtable.NumberOfEntries)
		}
	} else {
		for i := 0; i < conf.Memtable.NumberOfMemtables; i++ {
			memtables[i] = NewMemtable(false, 0, conf.Memtable.NumberOfEntries)
		}
	}
	return &Memtables{
		numberOfMemtables: conf.Memtable.NumberOfMemtables,
		memtables:         memtables,
		conf:              conf,
	}
}

// CRUD operacije
// Update dodaje ili azurira na osnovu kljuca u Memtables
func (m *Memtables) Update(key []byte, value []byte, timestamp int64, tombstone bool) {
	// Prolazimo kroz sve Memtable i azuriramo
	firstNotFull := -1
	// Trazimo Memtable koji sadrzi dati kljuc i uaput proveravamo koji je prvi koji nije pun
	// Ako ne nadjemo, onda cemo ga dodati u prvi koji nije pun
	for i := 0; i < m.numberOfMemtables; i++ {
		memtable := m.memtables[i]
		// Proveravamo da li postoji dati kljuc
		_, exist := memtable.Search(key)
		if exist {
			// Ako postoji, azuriramo vrednost
			memtable.Update(key, value, timestamp, tombstone)
			return
		} else {
			if firstNotFull == -1 && memtable.size < memtable.capacity {
				firstNotFull = i
			}
		}
	}
	// Ako nismo nasli Memtable koji sadrzi dati kljuc, a imamo prvi koji nije pun, onda ga azuriramo
	if firstNotFull != -1 {
		m.memtables[firstNotFull].Update(key, value, timestamp, tombstone)
	}
	if firstNotFull == m.numberOfMemtables-1 {
		if m.memtables[firstNotFull].size >= m.memtables[firstNotFull].capacity {
			// Ako je poslednji Memtable pun, onda ga flush-ujemo na disk
			m.memtables[0].FlushToDisk()
			// Resetujemo redosled Memtable-a
			for j := 0; j < m.numberOfMemtables-1; j++ {
				m.memtables[j] = m.memtables[j+1]
			}
			// Dodajemo novi Memtable na kraj
			m.memtables[m.numberOfMemtables-1] = NewMemtable(m.conf.Memtable.Structure == "skiplist", m.conf.Skiplist.MaxHeight, m.conf.Memtable.NumberOfEntries)
		}
	}

}

// Delete uklanja kljuc iz Memtables
func (m *Memtables) Delete(key []byte) {
	// Prolazimo kroz sve Memtable i brisemo
	for i := 0; i < m.numberOfMemtables; i++ {
		memtable := m.memtables[i]
		// Proveravamo da li postoji dati kljuc
		_, exist := memtable.Search(key)
		if exist {
			// Ako postoji, brisemo ga
			memtable.Delete(key)
			return
		}
	}
}

// Search trazi kljuc u Memtables
func (m *Memtables) Search(key []byte) ([]byte, bool) {
	// Prolazimo kroz sve Memtable i trazimo
	for i := 0; i < m.numberOfMemtables; i++ {
		memtable := m.memtables[i]
		// Proveravamo da li postoji dati kljuc
		value, exist := memtable.Search(key)
		if exist {
			// Ako postoji, vracamo vrednost
			return value, true
		}
	}
	// Ako nismo nasli kljuc, vracamo false
	return nil, false
}

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
		m.size++
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
