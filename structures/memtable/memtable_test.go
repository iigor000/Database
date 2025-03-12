package memtable

import (
	"fmt"
	"testing"
)

func TestMemtableCRUD(t *testing.T) {
	m := NewMemtable(true, 3, 9)
	m.Create(1, []byte("one"))
	m.Create(2, []byte("two"))
	m.Create(3, []byte("three"))
	m.Create(4, []byte("four"))
	m.Create(5, []byte("five"))
	m.Create(7, []byte("seven"))
	m.Create(10, []byte("ten"))
	m.Create(12, []byte("twelve"))

	value, found := m.Read(1)
	if !found || string(value) != "one" {
		t.Error("Expected one")
	}
	value, found = m.Read(4)
	if !found || string(value) != "four" {
		t.Error("Expected four")
	}
	value, found = m.Read(7)
	if !found || string(value) != "seven" {
		t.Error("Expected seven")
	}
	value, found = m.Read(10)
	if !found || string(value) != "ten" {
		t.Error("Expected ten")
	}
	value, found = m.Read(12)
	if !found || string(value) != "twelve" {
		t.Error("Expected twelve")
	}

	m.Delete(4)
	value, found = m.Read(4)
	if found {
		t.Error("Expected empty")
	}
	m.Delete(7)
	value, found = m.Read(7)
	if found {
		t.Error("Expected empty")
	}
	m.Delete(12)
	value, found = m.Read(12)
	if found {
		t.Error("Expected empty")
	}
	m.Print()
}

func TestMemtableFlush(t *testing.T) {
	m := NewMemtable(true, 3, 7)
	m.Create(1, []byte("one"))
	m.Create(2, []byte("two"))
	m.Create(3, []byte("three"))
	m.Create(4, []byte("four"))
	m.Create(5, []byte("five"))
	m.Create(7, []byte("seven"))
	m.Create(10, []byte("ten"))
	fmt.Println("Before flush")
	m.Print()
	m.Create(12, []byte("twelve"))
	fmt.Println("After flush")
	m.Print()
}
