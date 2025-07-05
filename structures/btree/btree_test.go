package btree

import (
	"bytes"
	"testing"
)

// Testira kreiranje novog b stabla
func TestNewBTree(t *testing.T) {
	tree := NewBTree(2)
	if tree == nil {
		t.Fatal("NewBTree returned nil") // prekida dalje testiranje
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
	tree.Insert(10, []byte("value10"))
	if tree.root == nil {
		t.Error("Root should not be nil after insertion")
	}
	if len(tree.root.keys) != 1 || tree.root.keys[0] != 10 {
		t.Error("Root key not inserted correctly")
	}
	if !bytes.Equal(tree.root.values[0], []byte("value10")) {
		t.Error("Value not inserted correctly")
	}

	// Pretraga postojecih i nepostojecih kljuceva
	val := tree.Search(10)
	if !bytes.Equal(val, []byte("value10")) {
		t.Error("Search returned incorrect value")
	}
	val = tree.Search(20)
	if val != nil {
		t.Error("Search should return nil for non-existent key")
	}

	// Dodavanje vise kljuceva
	tree.Insert(20, []byte("value20"))
	tree.Insert(5, []byte("value5"))
	tree.Insert(15, []byte("value15"))
	tree.Insert(25, []byte("value25"))

	testCases := []struct {
		key   byte
		value []byte
	}{
		{5, []byte("value5")},
		{10, []byte("value10")},
		{15, []byte("value15")},
		{20, []byte("value20")},
		{25, []byte("value25")},
	}

	for _, tc := range testCases {
		val := tree.Search(tc.key)
		if !bytes.Equal(val, tc.value) {
			t.Errorf("Search for key %d returned incorrect value, expected %s, got %s", tc.key, tc.value, val)
		}
	}
}

// Testira podelu korena
func TestSplitRoot(t *testing.T) {
	tree := NewBTree(2)
	keys := []byte{10, 20, 30, 40, 50}

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
	keys := []byte{10, 20, 30, 40, 50, 25, 35, 5, 15, 45}
	for _, k := range keys {
		tree.Insert(k, []byte("value"+string(k)))
	}

	tree.Delete(5)
	if tree.Search(5) != nil {
		t.Error("Key 5 should be deleted")
	}
	tree.Delete(30)
	if tree.Search(30) != nil {
		t.Error("Key 30 should be deleted")
	}

	remainingKeys := []byte{10, 15, 20, 25, 35, 40, 45, 50}
	for _, k := range remainingKeys {
		if tree.Search(k) == nil {
			t.Errorf("Key %d should still exist after deletions", k)
		}
	}

	for _, k := range remainingKeys {
		tree.Delete(k)
		if tree.Search(k) != nil {
			t.Errorf("Key %d should be deleted", k)
		}
	}

	if tree.root != nil && len(tree.root.keys) > 0 {
		t.Error("Tree should be empty after deleting all keys")
	}
}

// Testira inorder obilazak stabla
func TestTraversal(t *testing.T) {
	tree := NewBTree(2)
	keys := []byte{50, 30, 70, 20, 40, 60, 80, 10, 25, 35, 45, 55, 65, 75, 85}
	for _, k := range keys {
		tree.Insert(k, []byte("value"+string(k)))
	}

	sorted := tree.SortedKeys()
	if len(sorted) != len(keys) {
		t.Errorf("Expected %d keys, got %d", len(keys), len(sorted))
	}

	for i := 1; i < len(sorted); i++ {
		if sorted[i-1] > sorted[i] {
			t.Errorf("Keys not in sorted order: %d > %d", sorted[i-1], sorted[i])
		}
	}

	keyMap := make(map[byte]bool)
	for _, k := range keys {
		keyMap[k] = true
	}
	for _, k := range sorted {
		if !keyMap[k] {
			t.Errorf("Key %d found in traversal but not in original set", k)
		}
	}
}

// Testira pozajmljivanje i spajanje cvorova
func TestBorrowAndMerge(t *testing.T) {
	tree := NewBTree(2)
	keys := []byte{10, 20, 30, 40, 50, 60, 70, 80, 90}
	for _, k := range keys {
		tree.Insert(k, []byte("value"+string(k)))
	}

	tree.Delete(10)
	tree.Delete(20)
	tree.Delete(30)

	if tree.Search(10) != nil || tree.Search(20) != nil || tree.Search(30) != nil {
		t.Error("Deleted keys should not exist")
	}

	remainingKeys := []byte{40, 50, 60, 70, 80, 90}
	for _, k := range remainingKeys {
		if tree.Search(k) == nil {
			t.Errorf("Key %d should still exist after deletions", k)
		}
	}
}

// Testira ivicne slucajeve
func TestEdgeCases(t *testing.T) {
	tree := NewBTree(2)

	tree.Insert(10, []byte("value1"))
	tree.Insert(10, []byte("value2"))
	val := tree.Search(10)
	if !bytes.Equal(val, []byte("value2")) {
		t.Error("Duplicate key should overwrite value")
	}

	tree.Delete(99) // nepostojeci kljuc - ne sme izazvati gresku

	emptyTree := NewBTree(2)
	if emptyTree.Search(10) != nil {
		t.Error("Search on empty tree should return nil")
	}
	emptyTree.Delete(10) // ne sme izazvati gresku
}
