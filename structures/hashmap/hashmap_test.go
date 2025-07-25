package hashmap

import "testing"

// Pravi se iterator i prolazi kroz sve kljuceve
func TestIterator(t *testing.T) {
	hm := NewHashMap()
	hm.Update([]byte("key1"), []byte("one"), 0, false)
	hm.Update([]byte("key2"), []byte("two"), 0, false)
	hm.Update([]byte("key3"), []byte("three"), 0, false)

	iter, err := hm.NewIterator()
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}

	count := 0
	for iter.Next() {
		value := iter.Value()
		if value == nil {
			t.Error("Expected non-nil value")
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

func TestRangeIterator(t *testing.T) {
	hm := NewHashMap()
	hm.Update([]byte("key1"), []byte("one"), 0, false)
	hm.Update([]byte("key2"), []byte("two"), 0, false)
	hm.Update([]byte("key3"), []byte("three"), 0, false)
	hm.Update([]byte("key4"), []byte("four"), 0, false)

	startKey := []byte("key2")
	endKey := []byte("key3")

	rangeIter, err := hm.NewRangeIterator(startKey, endKey)
	if err != nil {
		t.Fatalf("Failed to create range iterator: %v", err)
	}

	// Ocekivane vrednosti u opsegu (prvi se ne gleda, jer krecemo petlju sa iter.Next())
	count := 0
	for rangeIter.Next() {
		value := rangeIter.Value()
		if value == nil {
			t.Error("Expected non-nil value")
			continue
		}
		switch count {
		case 0:
			if string(value.Value) != "two" {
				t.Errorf("Expected 'two', got '%s'", value.Value)
			}
		case 1:
			if string(value.Value) != "three" {
				t.Errorf("Expected 'three', got '%s'", value.Value)
			}
		default:
			t.Error("Unexpected extra iteration")
		}
		count++
	}

	if count != 2 {
		t.Errorf("Expected 2 iterations, got %d", count)
	}
}

func TestPrefixIterator(t *testing.T) {
	hm := NewHashMap()
	hm.Update([]byte("key1"), []byte("one"), 0, false)
	hm.Update([]byte("key2"), []byte("two"), 0, false)
	hm.Update([]byte("key3"), []byte("three"), 0, false)

	prefix := []byte("key")
	prefixIter, err := hm.NewPrefixIterator(prefix)
	if err != nil {
		t.Fatal("Failed to create prefix iterator")
	}

	// Ocekivane vrednosti, jedan nije u listi, jer pocinjemo petlju sa iter.Next()
	count := 0
	for prefixIter.Next() {
		value := prefixIter.Value()
		if value == nil {
			t.Error("Expected non-nil value")
			continue
		}
		switch count {
		case 0:
			if string(value.Value) != "two" {
				t.Errorf("Expected 'two', got '%s'", value.Value)
			}
		case 1:
			if string(value.Value) != "three" {
				t.Errorf("Expected 'three', got '%s'", value.Value)
			}
		default:
			t.Error("Unexpected extra iteration")
		}
		count++
	}

	if count != 2 {
		t.Errorf("Expected 2 iterations, got %d", count)
	}
}
