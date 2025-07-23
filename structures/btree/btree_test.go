package btree

import (
	"bytes"
	"testing"
	"time"

	memtable "github.com/iigor000/database/structures/adapter"
)

func createTestEntry(key, value []byte, tombstone bool) memtable.MemtableEntry {
	return memtable.MemtableEntry{
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
		Tombstone: tombstone,
	}
}

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
	if tree == nil {
		t.Fatal("Tree is nil")
	}

	// Prvo umetanje
	entry1 := createTestEntry([]byte("key1"), []byte("value1"), false)
	tree.Update(entry1.Key, entry1.Value, entry1.Timestamp, entry1.Tombstone)

	if tree.root == nil {
		t.Fatal("Root should not be nil after insertion")
	}

	if len(tree.root.keys) == 0 {
		t.Fatal("Root keys are not initialized")
	}

	if len(tree.root.keys) != 1 || !bytes.Equal(tree.root.keys[0], []byte("key1")) {
		t.Error("Root key not inserted correctly")
	}

	// Pretraga postojecih i nepostojecih kljuceva
	foundEntry, found := tree.Search([]byte("key1"))
	if !found {
		t.Error("Key should be found")
	}
	if !bytes.Equal(foundEntry.Value, []byte("value1")) {
		t.Error("Search returned incorrect value")
	}

	_, found = tree.Search([]byte("nonexistent"))
	if found {
		t.Error("Search should return false for non-existent key")
	}

	// Dodavanje vise kljuceva
	entries := []memtable.MemtableEntry{
		createTestEntry([]byte("key2"), []byte("value2"), false),
		createTestEntry([]byte("key0"), []byte("value0"), false),
		createTestEntry([]byte("key1.5"), []byte("value1.5"), false),
		createTestEntry([]byte("key3"), []byte("value3"), true),
	}

	for _, entry := range entries {
		tree.Update(entry.Key, entry.Value, entry.Timestamp, entry.Tombstone)
	}

	testCases := []struct {
		key       []byte
		value     []byte
		tombstone bool
	}{
		{[]byte("key0"), []byte("value0"), false},
		{[]byte("key1"), []byte("value1"), false},
		{[]byte("key1.5"), []byte("value1.5"), false},
		{[]byte("key2"), []byte("value2"), false},
		{[]byte("key3"), []byte("value3"), true},
	}

	for _, tc := range testCases {
		entry, found := tree.Search(tc.key)
		if !found {
			t.Errorf("Key %s should be found", tc.key)
			continue
		}
		if !bytes.Equal(entry.Value, tc.value) {
			t.Errorf("For key %s expected value %s, got %s", tc.key, tc.value, entry.Value)
		}
		if entry.Tombstone != tc.tombstone {
			t.Errorf("For key %s expected tombstone %t, got %t", tc.key, tc.tombstone, entry.Tombstone)
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
		entry := createTestEntry(k, []byte("value"+string(k)), false)
		tree.Update(entry.Key, entry.Value, entry.Timestamp, entry.Tombstone)

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
		entry := createTestEntry(k, []byte("value"+string(k)), false)
		tree.Update(entry.Key, entry.Value, entry.Timestamp, entry.Tombstone)
	}

	tree.Delete([]byte("key0.5"))
	if _, found := tree.Search([]byte("key0.5")); found {
		t.Error("Key 'key0.5' should be deleted")
	}

	tree.Delete([]byte("key3"))
	if _, found := tree.Search([]byte("key3")); found {
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
		if _, found := tree.Search(k); !found {
			t.Errorf("Key %s should still exist after deletions", k)
		}
	}

	for _, k := range remainingKeys {
		tree.Delete(k)
		if _, found := tree.Search(k); found {
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

	// Dodaj kljuceve u stablo
	for _, k := range keys {
		entry := createTestEntry(k, []byte("value"+string(k)), false)
		tree.Update(entry.Key, entry.Value, entry.Timestamp, entry.Tombstone)
	}

	// Izbriši neke kljuxeve
	tree.Delete([]byte("key1"))
	tree.Delete([]byte("key2"))
	tree.Delete([]byte("key3"))

	// Proveri da li su kljuxevi obrisani
	if _, found := tree.Search([]byte("key1")); found {
		t.Error("Key 'key1' should be deleted")
	}
	if _, found := tree.Search([]byte("key2")); found {
		t.Error("Key 'key2' should be deleted")
	}
	if _, found := tree.Search([]byte("key3")); found {
		t.Error("Key 'key3' should be deleted")
	}

	// potvrdi da ostali kljucevi postoje
	remainingKeys := [][]byte{
		[]byte("key4"),
		[]byte("key5"),
		[]byte("key6"),
		[]byte("key7"),
		[]byte("key8"),
		[]byte("key9"),
	}

	for _, k := range remainingKeys {
		if _, found := tree.Search(k); !found {
			t.Errorf("Key %s should still exist after deletions", k)
		}
	}
}

// Testira granicne slucajeve
func TestEdgeCases(t *testing.T) {
	tree := NewBTree(2)
	now := time.Now().UnixNano()

	// Testiraj dupliranje ključeva
	// Ocekuje se da se vrednost azurira, a ne da se dodaje novi cvor
	entry1 := createTestEntry([]byte("key1"), []byte("value1"), false)
	tree.Update(entry1.Key, entry1.Value, entry1.Timestamp, entry1.Tombstone)

	entry2 := createTestEntry([]byte("key1"), []byte("value2"), false)
	tree.Update(entry2.Key, entry2.Value, entry2.Timestamp, entry2.Tombstone)

	foundEntry, found := tree.Search([]byte("key1"))
	if !found {
		t.Error("Key should exist")
	}
	if !bytes.Equal(foundEntry.Value, []byte("value2")) {
		t.Error("Duplicate key should overwrite value")
	}

	// Testiraj brisanje nepostojeceg kljuca
	// Ocekuje se da ne dođe do greske
	tree.Delete([]byte("nonexistent"))

	emptyTree := NewBTree(2)
	if _, found := emptyTree.Search([]byte("key1")); found {
		t.Error("Search on empty tree should return not found")
	}

	// Testiraj brisanje iz praznog stabla
	emptyTree.Delete([]byte("key1"))

	// Test nil kljuca
	tree2 := NewBTree(2)
	validEntry := createTestEntry([]byte("valid"), []byte("value"), false)
	tree2.Update(validEntry.Key, validEntry.Value, validEntry.Timestamp, validEntry.Tombstone)

	tree2.Update(nil, []byte("value"), now, false)

	// Test tombstone azuriranje
	tree3 := NewBTree(2)
	entry3 := createTestEntry([]byte("key"), []byte("value"), false)
	tree3.Update(entry3.Key, entry3.Value, entry3.Timestamp, entry3.Tombstone)

	// Azriraj tombstone
	tree3.Update([]byte("key"), nil, now, true)

	entry, found := tree3.Search([]byte("key"))
	if !found {
		t.Error("Tombstoned key should still be found")
	}
	if !entry.Tombstone {
		t.Error("Entry should be tombstoned")
	}
}

func TestUpdate(t *testing.T) {
	tree := NewBTree(2)
	now := time.Now().UnixNano()

	tree.Update([]byte("key1"), []byte("value1"), now, false)

	// Verifikuj da li je kljuc umetnut i da li je vrednost odgovara
	entry, found := tree.Search([]byte("key1"))
	if !found {
		t.Error("Key should have been inserted")
	}
	if !bytes.Equal(entry.Value, []byte("value1")) {
		t.Error("Value was not set correctly")
	}

	// Test azuriranje postojeceg kljuca
	newValue := []byte("new_value1")
	tree.Update([]byte("key1"), newValue, now+1, false)

	// Da li je vrednost azurirana
	entry, _ = tree.Search([]byte("key1"))
	if !bytes.Equal(entry.Value, newValue) {
		t.Error("Value was not updated correctly")
	}

	// Test tombstone azuranje
	tree.Update([]byte("key1"), nil, now+2, true)

	// verifikuj tombstone
	entry, _ = tree.Search([]byte("key1"))
	if !entry.Tombstone {
		t.Error("Entry should be tombstoned")
	}
}
