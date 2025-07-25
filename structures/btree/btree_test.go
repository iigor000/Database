package btree

import (
	"bytes"
	"fmt"

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

	// Test update prazno stablo
	tree1 := NewBTree(2)
	if tree1.Update([]byte("key"), []byte("value")) {
		t.Error("Update on empty tree should return false")
	}

	// Test nil key
	tree2 := NewBTree(2)
	tree2.Insert([]byte("key"), []byte("value"))
	if tree2.Update(nil, []byte("value")) {
		t.Error("Update with nil key should return false")
	}

	// Test azivanje na nil vrednost
	tree3 := NewBTree(2)
	tree3.Insert([]byte("key"), []byte("value"))
	if !tree3.Update([]byte("key"), nil) {
		t.Error("Update to nil value should return true")
	}
	if tree3.Search([]byte("key")) != nil {
		t.Error("Search should return nil after updating value to nil")
	}
}

func TestUpdate(t *testing.T) {
	tree := NewBTree(2)

	// Test updating non-existent key (should return false)
	if tree.Update([]byte("key1"), []byte("value1")) {
		t.Error("Update should return false for non-existent key")
	}

	// Insert some keys
	tree.Insert([]byte("key1"), []byte("value1"))
	tree.Insert([]byte("key2"), []byte("value2"))
	tree.Insert([]byte("key3"), []byte("value3"))

	// Test updating existing keys
	testCases := []struct {
		key         []byte
		newValue    []byte
		shouldExist bool
	}{
		{[]byte("key1"), []byte("new_value1"), true},
		{[]byte("key2"), []byte("new_value2"), true},
		{[]byte("key3"), []byte("new_value3"), true},
		{[]byte("key4"), []byte("value4"), false}, // Doesn't exist
	}

	for _, tc := range testCases {
		updated := tree.Update(tc.key, tc.newValue)
		if updated != tc.shouldExist {
			t.Errorf("Update returned %v for key %s, expected %v", updated, tc.key, tc.shouldExist)
		}

		// Verify the value was updated if it should exist
		if tc.shouldExist {
			val := tree.Search(tc.key)
			if !bytes.Equal(val, tc.newValue) {
				t.Errorf("Value for key %s not updated correctly, expected %s, got %s",
					tc.key, tc.newValue, val)
			}
		}
	}

	// Test updating after splitting nodes
	for i := 4; i < 20; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		tree.Insert(key, key)
	}

	// Update keys in different nodes
	updateCases := []struct {
		key      []byte
		newValue []byte
	}{
		{[]byte("key1"), []byte("updated1")},
		{[]byte("key2"), []byte("updated2")},
		{[]byte("key4"), []byte("updated_4")},
		{[]byte("key10"), []byte("updated_10")},
	}

	for _, tc := range updateCases {
		if !tree.Update(tc.key, tc.newValue) {
			t.Errorf("Failed to update key %s that should exist", tc.key)
		}
		val := tree.Search(tc.key)
		if !bytes.Equal(val, tc.newValue) {
			t.Errorf("Value for key %s not updated correctly after split, expected %s, got %s",
				tc.key, tc.newValue, val)
		}
	}

	// Test update doesn't affect tree structure
	originalKeys := tree.SortedKeys()
	for _, tc := range updateCases {
		if !tree.Update(tc.key, []byte("final_update")) {
			t.Errorf("Failed to update key %s in final check", tc.key)
		}
	}
	newKeys := tree.SortedKeys()

	if len(originalKeys) != len(newKeys) {
		t.Errorf("Update changed number of keys in tree, was %d, now %d",
			len(originalKeys), len(newKeys))
	}

	for i := range originalKeys {
		if !bytes.Equal(originalKeys[i], newKeys[i]) {
			t.Errorf("Update changed keys in tree, at index %d was %s, now %s",
				i, originalKeys[i], newKeys[i])
		}
	}
}

func TestBTreeIterator(t *testing.T) {
	t.Run("EmptyTree", func(t *testing.T) {
		tree := NewBTree(2)
		_, err := tree.NewIterator()
		if err == nil {
			t.Error("Expected error for empty tree")
		}
	})

	t.Run("FullIteration", func(t *testing.T) {
		tree := NewBTree(2)
		entries := []struct {
			key   []byte
			value []byte
		}{
			{[]byte("d"), []byte("delta")},
			{[]byte("b"), []byte("bravo")},
			{[]byte("f"), []byte("foxtrot")},
			{[]byte("a"), []byte("alpha")},
			{[]byte("c"), []byte("charlie")},
			{[]byte("e"), []byte("echo")},
		}

		// Dodaj u neuredjenom redosledu
		for _, entry := range entries {
			tree.Insert(entry.key, entry.value)
		}

		iter, err := tree.NewIterator()
		if err != nil {
			t.Fatalf("Failed to create iterator: %v", err)
		}

		expectedOrder := []string{"a", "b", "c", "d", "e", "f"}
		index := 0

		for iter.Next() {
			if index >= len(expectedOrder) {
				t.Error("Iterator returned more items than expected")
				break
			}

			key, _ := iter.Value()
			if string(key) != expectedOrder[index] {
				t.Errorf("Expected key %s, got %s", expectedOrder[index], key)
			}
			index++
		}

		if index != len(expectedOrder) {
			t.Errorf("Expected %d items, got %d", len(expectedOrder), index)
		}
	})

	t.Run("RangeIteration", func(t *testing.T) {
		tree := NewBTree(2)
		entries := []struct {
			key   []byte
			value []byte
		}{
			{[]byte("apple"), []byte("fruit")},
			{[]byte("banana"), []byte("fruit")},
			{[]byte("carrot"), []byte("vegetable")},
			{[]byte("date"), []byte("fruit")},
			{[]byte("eggplant"), []byte("vegetable")},
			{[]byte("fig"), []byte("fruit")},
		}

		for _, entry := range entries {
			tree.Insert(entry.key, entry.value)
		}

		// Test range od "banana" do "eggplant" (ukljucujuci)
		iter, err := tree.NewRangeIterator([]byte("banana"), []byte("eggplant"))
		if err != nil {
			t.Fatalf("Failed to create range iterator: %v", err)
		}

		expectedOrder := []string{"banana", "carrot", "date", "eggplant"}
		index := 0

		for iter.Next() {
			if index >= len(expectedOrder) {
				t.Error("Range iterator returned more items than expected")
				break
			}

			key, _ := iter.Value()
			if string(key) != expectedOrder[index] {
				t.Errorf("Expected key %s, got %s", expectedOrder[index], key)
			}
			index++
		}

		if index != len(expectedOrder) {
			t.Errorf("Expected %d items in range, got %d", len(expectedOrder), index)
		}

		// Test prazan range
		emptyIter, err := tree.NewRangeIterator([]byte("mango"), []byte("orange"))
		if err != nil {
			t.Fatalf("Failed to create empty range iterator: %v", err)
		}

		if emptyIter.Next() {
			t.Error("Expected empty range iterator to return false immediately")
		}
	})

	t.Run("PrefixIteration", func(t *testing.T) {
		tree := NewBTree(2)
		entries := []struct {
			key   []byte
			value []byte
		}{
			{[]byte("apple"), []byte("fruit")},
			{[]byte("apricot"), []byte("fruit")},
			{[]byte("banana"), []byte("fruit")},
			{[]byte("blueberry"), []byte("fruit")},
			{[]byte("blackberry"), []byte("fruit")},
			{[]byte("cherry"), []byte("fruit")},
		}

		for _, entry := range entries {
			tree.Insert(entry.key, entry.value)
		}

		// Test prefix "b"
		iter, err := tree.NewPrefixIterator([]byte("b"))
		if err != nil {
			t.Fatalf("Failed to create prefix iterator: %v", err)
		}

		expectedOrder := []string{"banana", "blackberry", "blueberry"}
		index := 0

		for iter.Next() {
			if index >= len(expectedOrder) {
				t.Error("Prefix iterator returned more items than expected")
				break
			}

			key, _ := iter.Value()
			if string(key) != expectedOrder[index] {
				t.Errorf("Expected key %s, got %s", expectedOrder[index], key)
			}
			index++
		}

		if index != len(expectedOrder) {
			t.Errorf("Expected %d items with prefix, got %d", len(expectedOrder), index)
		}

	})

	t.Run("ConcurrentModification", func(t *testing.T) {
		tree := NewBTree(2)
		tree.Insert([]byte("a"), []byte("1"))
		tree.Insert([]byte("b"), []byte("2"))

		iter, err := tree.NewIterator()
		if err != nil {
			t.Fatalf("Failed to create iterator: %v", err)
		}

		// modifikuj stablo tokom iteracije
		tree.Insert([]byte("c"), []byte("3"))

		count := 0
		for iter.Next() {
			count++
			key, value := iter.Value()
			fmt.Printf("Key: %s, Value: %s\n", key, value)
		}

		if count != 2 {
			t.Errorf("Expected iterator to see 2 items, saw %d", count)
		}

		// New iterator treba da vidi sve
		newIter, err := tree.NewIterator()
		if err != nil {
			t.Fatalf("Failed to create new iterator: %v", err)
		}

		newCount := 0
		for newIter.Next() {
			newCount++
		}

		if newCount != 3 {
			t.Errorf("Expected new iterator to see 3 items, saw %d", newCount)
		}
	})

	t.Run("EdgeCases", func(t *testing.T) {
		tree := NewBTree(2)
		tree.Insert([]byte("a"), []byte("1"))
		tree.Insert([]byte("b"), []byte("2"))
		tree.Insert([]byte("c"), []byte("3"))

		// Test range gde je start==end
		iter, err := tree.NewRangeIterator([]byte("b"), []byte("b"))
		if err != nil {
			t.Fatalf("Failed to create range iterator: %v", err)
		}

		if !iter.Next() {
			t.Error("Expected to find single item in start==end range")
		}

		key, _ := iter.Value()
		if string(key) != "b" {
			t.Errorf("Expected key 'b', got %s", key)
		}

		if iter.Next() {
			t.Error("Expected only one item in start==end range")
		}

		// Test prazan prefix
		prefixIter, err := tree.NewPrefixIterator([]byte(""))
		if err != nil {
			t.Fatalf("Failed to create prefix iterator: %v", err)
		}

		prefixCount := 0
		for prefixIter.Next() {
			prefixCount++
		}

		if prefixCount != 3 {
			t.Errorf("Expected empty prefix to match all items, got %d", prefixCount)
		}
	})
}
