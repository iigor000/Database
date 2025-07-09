package memtable

import (
	"fmt"
	"testing"

	"github.com/iigor000/database/config"
)

func TestMemtableCRUD(t *testing.T) {
	m := NewMemtable(true, 3, 9)
	// Test initial state
	if len(m.Keys) != 0 {
		t.Error("Expected empty Memtable at initialization")
	}
	// Test Update and Search
	fmt.Println("Adding entries to Memtable:")
	m.Update([]byte("1"), []byte("one"), 1, false)
	//fmt.Println("Added key 1 with value 'one'")
	m.Update([]byte("4"), []byte("four"), 4, false)
	m.Update([]byte("7"), []byte("seven"), 7, false)
	m.Update([]byte("10"), []byte("ten"), 10, false)
	m.Update([]byte("12"), []byte("twelve"), 12, false)
	fmt.Println("Added keys 1, 4, 7, 10, 12")
	fmt.Println("Updating keys 1, 4, 7, 12 with new values:")
	m.Update([]byte("1"), []byte("newone"), 13, false)
	m.Update([]byte("4"), []byte("newfour"), 14, false)
	m.Update([]byte("7"), []byte("newseven"), 15, false)
	m.Update([]byte("12"), []byte("newtwelve"), 16, false)
	fmt.Println("Updated keys 1, 4, 7, 12 with new values")

	value, found := m.Search([]byte("1"))
	if !found || string(value) != "newone" {
		t.Error("Expected newone")
	}
	value, found = m.Search([]byte("4"))
	if !found || string(value) != "newfour" {
		t.Error("Expected newfour")
	}
	value, found = m.Search([]byte("7"))
	if !found || string(value) != "newseven" {
		t.Error("Expected newseven")
	}
	value, found = m.Search([]byte("12"))
	if !found || string(value) != "newtwelve" {
		t.Error("Expected newtwelve")
	}
	value, found = m.Search([]byte("10"))
	if !found || string(value) != "ten" {
		t.Error("Expected ten")
	}
	_, found = m.Search([]byte("2"))
	if found {
		t.Error("Expected not found for key 2")
	}
	fmt.Println("Search test passed")

	fmt.Println("Deleting 4, 10, and 12 from Memtable:")
	m.Delete([]byte("4"))
	_, found = m.Search([]byte("4"))
	if found {
		t.Error("Expected not found for deleted key 4")
	}
	m.Delete([]byte("10"))
	_, found = m.Search([]byte("10"))
	if found {
		t.Error("Expected not found for deleted key 10")
	}
	m.Delete([]byte("12"))
	_, found = m.Search([]byte("12"))
	if found {
		t.Error("Expected not found for deleted key 12")
	}

	fmt.Println("Memtable contents after CRUD operations:")
	m.Print()
}

func TestMemtables(t *testing.T) {
	cf := config.Config{Memtable: config.MemtableConfig{
		NumberOfMemtables: 2,
		NumberOfEntries:   5,
		Structure:         "skiplist",
	},
		Skiplist: config.SkiplistConfig{
			MaxHeight: 3},
	}
	ms := NewMemtables(&cf)
	for i := 0; i < cf.Memtable.NumberOfMemtables; i++ {
		fmt.Printf("Memtable %d:\n", i)
		ms.Memtables[i].Print()
	}
	fmt.Println("Adding entries to Memtables:")
	ms.Update([]byte("1"), []byte("one"), 1, false)
	ms.Update([]byte("2"), []byte("two"), 2, false)
	ms.Update([]byte("3"), []byte("three"), 3, false)
	ms.Update([]byte("4"), []byte("four"), 4, false)
	ms.Update([]byte("5"), []byte("five"), 5, false)
	ms.Update([]byte("6"), []byte("six"), 6, false)
	ms.Update([]byte("7"), []byte("seven"), 7, false)
	ms.Update([]byte("8"), []byte("eight"), 8, false)
	for i := 0; i < cf.Memtable.NumberOfMemtables; i++ {
		fmt.Printf("Memtable %d after updates:\n", i)
		ms.Memtables[i].Print()
	}
	fmt.Println("Updating keys in Memtables:")
	ms.Update([]byte("1"), []byte("newone"), 10, false)
	ms.Update([]byte("2"), []byte("newtwo"), 11, false)
	ms.Update([]byte("3"), []byte("newthree"), 12, false)
	ms.Update([]byte("4"), []byte("newfour"), 13, false)
	ms.Update([]byte("5"), []byte("newfive"), 14, false)
	ms.Update([]byte("6"), []byte("newsix"), 15, false)
	ms.Update([]byte("7"), []byte("newseven"), 16, false)
	ms.Update([]byte("8"), []byte("neweight"), 17, false)
	for i := 0; i < cf.Memtable.NumberOfMemtables; i++ {
		fmt.Printf("Memtable %d after updates:\n", i)
		ms.Memtables[i].Print()
	}
	fmt.Println("Searching keys in Memtables:")
	value, found := ms.Search([]byte("1"))
	if !found {
		t.Error("Expected to find key 1")
	} else if string(value) != "newone" {
		t.Error("Expected value 'newone' for key 1, got", string(value))
	}
	value, found = ms.Search([]byte("2"))
	if !found {
		t.Error("Expected to find key 2")
	}
	if string(value) != "newtwo" {
		t.Error("Expected value 'newtwo' for key 2, got", string(value))
	}
	fmt.Println("Deleting keys from Memtables:")
	ms.Delete([]byte("1"))
	_, found = ms.Search([]byte("1"))
	if found {
		t.Error("Expected not found for deleted key 1")
	}
	ms.Delete([]byte("2"))
	_, found = ms.Search([]byte("2"))
	if found {
		t.Error("Expected not found for deleted key 2")
	}
	ms.Delete([]byte("3"))
	_, found = ms.Search([]byte("3"))
	if found {
		t.Error("Expected not found for deleted key 3")
	}
	fmt.Println("Memtables contents after CRUD operations:")
	for i := 0; i < cf.Memtable.NumberOfMemtables; i++ {
		fmt.Printf("Memtable %d:\n", i)
		ms.Memtables[i].Print()
	}

}

// Flush Memtables to disk
func TestFlushMemtables(t *testing.T) {
	cf := config.Config{Memtable: config.MemtableConfig{
		NumberOfMemtables: 2,
		NumberOfEntries:   2,
		Structure:         "skiplist",
	},
		Skiplist: config.SkiplistConfig{
			MaxHeight: 3},
	}
	ms := NewMemtables(&cf)
	// Add some entries to Memtables
	ms.Update([]byte("1"), []byte("one"), 1, false)
	ms.Update([]byte("2"), []byte("two"), 2, false)
	ms.Update([]byte("3"), []byte("three"), 3, false)
	// Print Memtables before flush
	for i := 0; i < cf.Memtable.NumberOfMemtables; i++ {
		fmt.Printf("Memtable %d before flush:\n", i)
		ms.Memtables[i].Print()
	}
	ms.Update([]byte("4"), []byte("four"), 4, false)
	ms.Update([]byte("5"), []byte("five"), 5, false)
	// Print Memtables after flush
	for i := 0; i < cf.Memtable.NumberOfMemtables; i++ {
		fmt.Printf("Memtable %d after flush:\n", i)
		ms.Memtables[i].Print()
	}
}
