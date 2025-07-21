package btree

import (
	"bytes"
	"testing"
)

// Testira kreiranje novog b stabla
func TestNewBTree(t *testing.T) {
	tree := NewBTree(2)
	if tree == nil {
		t.Fatal("NewBTree returned nil")
	}

	if tree.root != nil {
		t.Error("New tree should have nil root")
	}
	if tree.t != 2 {
		t.Error("Tree degree not set correctly")
	}
}

// Testira umetanje i pretragu
func TestInsertAndSearch(t *testing.T) {
	tree := NewBTree(2)

	// Prvo umetanje
	tree.Insert([]byte("key1"), []byte("value1"))
	if tree.root == nil {
		t.Error("Root should not be nil after insertion")
	}
	if len(tree.root.keys) != 1 || !bytes.Equal(tree.root.keys[0], []byte("key1")) {
		t.Error("Root key not inserted correctly")
	}
	if !bytes.Equal(tree.root.values[0], []byte("value1")) {
		t.Error("Value not inserted correctly")
	}

	// Pretraga postojecih i nepostojecih kljuceva
	val := tree.Search([]byte("key1"))
	if !bytes.Equal(val, []byte("value1")) {
		t.Error("Search returned incorrect value")
	}
	val = tree.Search([]byte("nonexistent"))
	if val != nil {
		t.Error("Search should return nil for non-existent key")
	}

	// Dodavanje vise kljuceva
	tree.Insert([]byte("key2"), []byte("value2"))
	tree.Insert([]byte("key0"), []byte("value0"))
	tree.Insert([]byte("key1.5"), []byte("value1.5"))
	tree.Insert([]byte("key3"), []byte("value3"))

	testCases := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key0"), []byte("value0")},
		{[]byte("key1"), []byte("value1")},
		{[]byte("key1.5"), []byte("value1.5")},
		{[]byte("key2"), []byte("value2")},
		{[]byte("key3"), []byte("value3")},
	}

	for _, tc := range testCases {
		val := tree.Search(tc.key)
		if !bytes.Equal(val, tc.value) {
			t.Errorf("Search for key %s returned incorrect value, expected %s, got %s", tc.key, tc.value, val)
		}
	}
}

// Testira podelu korena
func TestSplitRoot(t *testing.T) {
	tree := NewBTree(2)
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
		[]byte("key5"),
	}

	for i, k := range keys {
		tree.Insert(k, []byte("value"+string(k)))

		if i == 2 {
			if len(tree.root.keys) != 3 {
				t.Errorf("Expected 3 keys in root before split, got %d", len(tree.root.keys))
			}
		}
		if i == 3 {
			if len(tree.root.keys) != 1 {
				t.Errorf("Expected root to have 1 key after split, got %d", len(tree.root.keys))
			}
			if len(tree.root.children) != 2 {
				t.Errorf("Expected root to have 2 children after split, got %d", len(tree.root.children))
			}
		}
	}

	if len(tree.root.keys) != 1 {
		t.Error("Root should have exactly 1 key")
	}
	if len(tree.root.children) != 2 {
		t.Error("Root should have exactly 2 children")
	}
}

// Testira brisanje elemenata iz stabla
func TestDelete(t *testing.T) {
	tree := NewBTree(2)
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
		[]byte("key5"),
		[]byte("key2.5"),
		[]byte("key3.5"),
		[]byte("key0.5"),
		[]byte("key1.5"),
		[]byte("key4.5"),
	}
	for _, k := range keys {
		tree.Insert(k, []byte("value"+string(k)))
	}

	tree.Delete([]byte("key0.5"))
	if tree.Search([]byte("key0.5")) != nil {
		t.Error("Key 'key0.5' should be deleted")
	}
	tree.Delete([]byte("key3"))
	if tree.Search([]byte("key3")) != nil {
		t.Error("Key 'key3' should be deleted")
	}

	remainingKeys := [][]byte{
		[]byte("key1"),
		[]byte("key1.5"),
		[]byte("key2"),
		[]byte("key2.5"),
		[]byte("key3.5"),
		[]byte("key4"),
		[]byte("key4.5"),
		[]byte("key5"),
	}
	for _, k := range remainingKeys {
		if tree.Search(k) == nil {
			t.Errorf("Key %s should still exist after deletions", k)
		}
	}

	for _, k := range remainingKeys {
		tree.Delete(k)
		if tree.Search(k) != nil {
			t.Errorf("Key %s should be deleted", k)
		}
	}

	if tree.root != nil && len(tree.root.keys) > 0 {
		t.Error("Tree should be empty after deleting all keys")
	}
}

// Testira inorder obilazak stabla
func TestTraversal(t *testing.T) {
	tree := NewBTree(2)
	keys := [][]byte{
		[]byte("key5"),
		[]byte("key3"),
		[]byte("key7"),
		[]byte("key2"),
		[]byte("key4"),
		[]byte("key6"),
		[]byte("key8"),
		[]byte("key1"),
		[]byte("key2.5"),
		[]byte("key3.5"),
		[]byte("key4.5"),
		[]byte("key5.5"),
		[]byte("key6.5"),
		[]byte("key7.5"),
		[]byte("key8.5"),
	}
	for _, k := range keys {
		tree.Insert(k, []byte("value"+string(k)))
	}

	sorted := tree.SortedKeys()
	if len(sorted) != len(keys) {
		t.Errorf("Expected %d keys, got %d", len(keys), len(sorted))
	}

	for i := 1; i < len(sorted); i++ {
		if bytes.Compare(sorted[i-1], sorted[i]) > 0 {
			t.Errorf("Keys not in sorted order: %s > %s", sorted[i-1], sorted[i])
		}
	}

	keyMap := make(map[string]bool)
	for _, k := range keys {
		keyMap[string(k)] = true
	}
	for _, k := range sorted {
		if !keyMap[string(k)] {
			t.Errorf("Key %s found in traversal but not in original set", k)
		}
	}
}

// Testira pozajmljivanje i spajanje cvorova
func TestBorrowAndMerge(t *testing.T) {
	tree := NewBTree(2)
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
		[]byte("key5"),
		[]byte("key6"),
		[]byte("key7"),
		[]byte("key8"),
		[]byte("key9"),
	}
	for _, k := range keys {
		tree.Insert(k, []byte("value"+string(k)))
	}

	tree.Delete([]byte("key1"))
	tree.Delete([]byte("key2"))
	tree.Delete([]byte("key3"))

	if tree.Search([]byte("key1")) != nil || tree.Search([]byte("key2")) != nil || tree.Search([]byte("key3")) != nil {
		t.Error("Deleted keys should not exist")
	}

	remainingKeys := [][]byte{
		[]byte("key4"),
		[]byte("key5"),
		[]byte("key6"),
		[]byte("key7"),
		[]byte("key8"),
		[]byte("key9"),
	}
	for _, k := range remainingKeys {
		if tree.Search(k) == nil {
			t.Errorf("Key %s should still exist after deletions", k)
		}
	}
}

// Testira ivicne slucajeve
func TestEdgeCases(t *testing.T) {
	tree := NewBTree(2)

	tree.Insert([]byte("key1"), []byte("value1"))
	tree.Insert([]byte("key1"), []byte("value2"))
	val := tree.Search([]byte("key1"))
	if !bytes.Equal(val, []byte("value2")) {
		t.Error("Duplicate key should overwrite value")
	}

	tree.Delete([]byte("nonexistent")) // nepostojeci kljuc - ne sme izazvati gresku

	emptyTree := NewBTree(2)
	if emptyTree.Search([]byte("key1")) != nil {
		t.Error("Search on empty tree should return nil")
	}
	emptyTree.Delete([]byte("key1")) // ne sme izazvati gresku
}
