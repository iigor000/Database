package compression

import (
	"testing"
)

func TestDictionary(t *testing.T) {
	dict := NewDictionary()

	// Test Add
	key0 := []byte("key0")
	index0 := dict.Add(key0)
	if index0 != 0 {
		t.Errorf("Expected index 0, got %d", index0)
	}
	key1 := []byte("key1")
	index1 := dict.Add(key1)
	if index1 != 1 {
		t.Errorf("Expected index 1, got %d", index1)
	}
	// Test SearchIndex
	if key, exists := dict.SearchIndex(0); !exists || string(key) != "key0" {
		t.Errorf("Expected key 'key0' at index 0, got %s", key)
	}
	if key, exists := dict.SearchIndex(1); !exists || string(key) != "key1" {
		t.Errorf("Expected key 'key1' at index 1, got %s", key)
	}
	if key, exists := dict.SearchIndex(2); exists {
		t.Errorf("Expected no key at index 2, got %s", key)
	}
	// Test SearchKey
	if index, exists := dict.SearchKey(key0); !exists || index != 0 {
		t.Errorf("Expected index 0 for key 'key0', got %d", index)
	}
	if index, exists := dict.SearchKey(key1); !exists || index != 1 {
		t.Errorf("Expected index 1 for key 'key1', got %d", index)
	}
	if index, exists := dict.SearchKey([]byte("key2")); exists {
		t.Errorf("Expected no index for key 'key2', got %d", index)
	}
}

// Test Encode and Decode
func TestEncodeDecode(t *testing.T) {
	dict := NewDictionary()
	key0 := []byte("key0")
	key1 := []byte("key1")
	dict.Add(key0)
	dict.Add(key1)

	encoded := dict.Encode()
	decoded, pass := Decode(encoded)
	if !pass {
		t.Fatal("Decode failed")
	}

	if len(decoded.keys) != 2 {
		t.Fatalf("Expected 2 keys, got %d", len(decoded.keys))
	}
	if string(decoded.keys[0]) != "key0" || string(decoded.keys[1]) != "key1" {
		t.Errorf("Decoded keys do not match original: %s, %s", decoded.keys[0], decoded.keys[1])
	}
}

// Test Read and Write to file
func TestReadWriteFile(t *testing.T) {
	dict := NewDictionary()
	key0 := []byte("key0")
	key1 := []byte("key1")
	dict.Add(key0)
	dict.Add(key1)
	// Write
	err := dict.Write("test_dict.db")
	if err != nil {
		t.Fatalf("WriteToFile failed: %v", err)
	}
	// Read
	decoded, err := Read("test_dict.db")
	if err != nil {
		t.Fatalf("ReadFromFile failed: %v", err)
	}

	if len(decoded.keys) != 2 {
		t.Fatalf("Expected 2 keys, got %d", len(decoded.keys))
	}
	if string(decoded.keys[0]) != "key0" || string(decoded.keys[1]) != "key1" {
		t.Errorf("Decoded keys do not match original: %s, %s", decoded.keys[0], decoded.keys[1])
	}
}
