package skiplist

import (
	"testing"
)

func TestList(t *testing.T) {
	s := MakeSkipList(3)
	s.Add([]byte("key1"), []byte("one"))
	s.Add([]byte("key2"), []byte("two"))
	s.Add([]byte("key3"), []byte("three"))
	s.Add([]byte("key4"), []byte("four"))
	s.Add([]byte("key5"), []byte("five"))
	s.Add([]byte("key7"), []byte("seven"))
	s.Add([]byte("key10"), []byte("ten"))
	s.Add([]byte("key12"), []byte("twelve"))

	if string(s.Search([]byte("key1"))) != "one" {
		t.Error("Expected one")
	}
	if string(s.Search([]byte("key4"))) != "four" {
		t.Error("Expected four")
	}
	if string(s.Search([]byte("key7"))) != "seven" {
		t.Error("Expected seven")
	}
	if string(s.Search([]byte("key10"))) != "ten" {
		t.Error("Expected ten")
	}
	if string(s.Search([]byte("key12"))) != "twelve" {
		t.Error("Expected twelve")
	}

	s.Remove([]byte("key4"))
	if string(s.Search([]byte("key4"))) != "" {
		t.Error("Expected empty")
	}
	s.Remove([]byte("key7"))
	if string(s.Search([]byte("key7"))) != "" {
		t.Error("Expected empty")
	}
	s.Remove([]byte("key12"))
	if string(s.Search([]byte("key12"))) != "" {
		t.Error("Expected empty")
	}
}
