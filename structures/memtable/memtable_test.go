package memtable

import (
	"fmt"
	"testing"
)

func TestMemtableCRUD(t *testing.T) {
	m := NewMemtable(true, 3, 9)
	// Test initial state
	if len(m.keys) != 0 {
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

func TestMemtableFlush(t *testing.T) {
	m := NewMemtable(true, 3, 7)
	m.Update([]byte("1"), []byte("one"), 1, false)
	m.Update([]byte("2"), []byte("two"), 2, false)
	m.Update([]byte("3"), []byte("three"), 3, false)
	m.Update([]byte("4"), []byte("four"), 4, false)
	m.Update([]byte("5"), []byte("five"), 5, false)
	m.Update([]byte("6"), []byte("six"), 6, false)
	m.Update([]byte("7"), []byte("seven"), 7, false)
	m.FlushToDisk()

}
