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

	if string(s.search([]byte("key1"))) != "one" {
		t.Error("Expected one")
	}
	if string(s.search([]byte("key4"))) != "four" {
		t.Error("Expected four")
	}
	if string(s.search([]byte("key7"))) != "seven" {
		t.Error("Expected seven")
	}
	if string(s.search([]byte("key10"))) != "ten" {
		t.Error("Expected ten")
	}
	if string(s.search([]byte("key12"))) != "twelve" {
		t.Error("Expected twelve")
	}

	s.Remove([]byte("key4"))
	if string(s.search([]byte("key4"))) != "" {
		t.Error("Expected empty")
	}
	s.Remove([]byte("key7"))
	if string(s.search([]byte("key7"))) != "" {
		t.Error("Expected empty")
	}
	s.Remove([]byte("key12"))
	if string(s.search([]byte("key12"))) != "" {
		t.Error("Expected empty")
	}
}

func TestBasicIterator(t *testing.T) {
	s := MakeSkipList(3)
	s.Create([]byte("key1"), []byte("one"), 0, false)
	s.Create([]byte("key2"), []byte("two"), 0, false)
	s.Create([]byte("key3"), []byte("three"), 0, false)

	println("=== Testing Basic Iterator ===")
	iter, err := s.NewIterator()
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}

	count := 0
	for {
		value, ok := iter.Next()
		if !ok {
			break
		}
		if len(value.Key) != 0 || len(value.Value) != 0 {
			println("Key:", string(value.Key), "Value:", string(value.Value))
			count++
		} else {
			println("Got nil value from iterator")
		}
	}
	println("Total items found:", count)

	if count != 3 {
		t.Errorf("Expected 3 items, got %d", count)
	}
}

func TestRangeIterator(t *testing.T) {
	s := MakeSkipList(3)
	s.Create([]byte("key1"), []byte("one"), 0, false)
	s.Create([]byte("key2"), []byte("two"), 0, false)
	s.Create([]byte("key3"), []byte("three"), 0, false)
	s.Create([]byte("key4"), []byte("four"), 0, false)
	s.Create([]byte("key5"), []byte("five"), 0, false)
	s.Create([]byte("key6"), []byte("six"), 0, false)
	s.Create([]byte("key7"), []byte("seven"), 0, false)
	s.Create([]byte("key8"), []byte("eight"), 0, false)
	s.Create([]byte("key9"), []byte("nine"), 0, false)
	s.Create([]byte("key10"), []byte("ten"), 0, false)
	s.Create([]byte("key11"), []byte("eleven"), 0, false)
	s.Create([]byte("key12"), []byte("twelve"), 0, false)

	// Pravimo range iterator od key3 do key9
	iter, err := s.NewRangeIterator([]byte("key3"), []byte("key9"))
	if err != nil {
		t.Fatalf("Failed to create range iterator: %v", err)
	}

	// Ocekujemo da cemo naci ove vrednosti, tri nije u listi, jer pocinjemo petlju sa iter.Next()
	expectedValues := []string{"three", "four", "five", "six", "seven", "eight", "nine"}
	i := 0
	for value, ok := iter.Next(); ok; value, ok = iter.Next() {
		println("Key:", string(value.Key), "Value:", string(value.Value))

		if value.Key == nil && value.Value == nil {
			t.Error("Expected non-nil value")
			continue
		}
		if string(value.Value) != expectedValues[i] {
			t.Errorf("Expected %s, got %s", expectedValues[i], value.Value)
		}
		i++
	}

	// Proveravamo da li je broj pronadjenih vrednosti jednak broju ocekivanih vrednosti
	println("Total values found:", i)
	if i != len(expectedValues) {
		t.Errorf("Expected %d values, got %d", len(expectedValues), i)
	}
}

func TestPrefixIterate(t *testing.T) {
	s := MakeSkipList(3)
	s.Create([]byte("key1"), []byte("one"), 0, false)
	s.Create([]byte("key2"), []byte("two"), 0, false)
	s.Create([]byte("key3"), []byte("three"), 0, false)

	prefix := []byte("key")
	prefixIter, err := s.NewPrefixIterator(prefix)
	if err != nil {
		t.Fatal("Failed to create prefix iterator")
	}

	// Ocekivane vrednosti, jedan nije u listi, jer pocinjemo petlju sa iter.Next()
	count := 0
	for value, ok := prefixIter.Next(); ok; value, ok = prefixIter.Next() {
		println("Key:", string(value.Key), "Value:", string(value.Value))
		if len(value.Key) == 0 && len(value.Value) == 0 {
			t.Error("Expected non-zero value")
			continue
		}
		switch count {
		case 0:
			if string(value.Value) != "one" {
				t.Errorf("Expected 'one', got '%s'", value.Value)
			}
		case 1:
			if string(value.Value) != "two" {
				t.Errorf("Expected 'two', got '%s'", value.Value)
			}
		case 2:
			if string(value.Value) != "three" {
				t.Errorf("Expected 'three', got '%s'", value.Value)
			}
		default:
			t.Error("Unexpected extra iteration")
		}
		count++
	}

	if count != 3 {
		t.Errorf("Expected 3 iterations, got %d", count)
	}
}
