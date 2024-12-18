package test

import (
	"testing"

	"github.com/iigor000/database/structures/skiplist"
)

func TestList(t *testing.T) {
	s := skiplist.MakeSkipList(3)
	s.Add(1, []byte("one"))
	s.Add(2, []byte("two"))
	s.Add(3, []byte("three"))
	s.Add(4, []byte("four"))
	s.Add(5, []byte("five"))
	s.Add(7, []byte("seven"))
	s.Add(10, []byte("ten"))
	s.Add(12, []byte("twelve"))

	if string(s.Search(1)) != "one" {
		t.Error("Expected one")
	}
	if string(s.Search(4)) != "four" {
		t.Error("Expected four")
	}
	if string(s.Search(7)) != "seven" {
		t.Error("Expected seven")
	}
	if string(s.Search(10)) != "ten" {
		t.Error("Expected ten")
	}
	if string(s.Search(12)) != "twelve" {
		t.Error("Expected twelve")
	}

	s.Remove(4)
	if string(s.Search(4)) != "" {
		t.Error("Expected empty")
	}
	s.Remove(7)
	if string(s.Search(7)) != "" {
		t.Error("Expected empty")
	}
	s.Remove(12)
	if string(s.Search(12)) != "" {
		t.Error("Expected empty")
	}
}
