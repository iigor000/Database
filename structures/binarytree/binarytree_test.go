package binarytree

import (
	"bytes"
	"testing"
)

// Pomocna funkcija - proverava da li su dva niza jednaka
func areByteSlicesEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func TestInsert(t *testing.T) {
	var root *Node

	keys := [][]byte{
		[]byte("10"),
		[]byte("23"),
		[]byte("35"),
		[]byte("7"),
		[]byte("12"),
		[]byte("31"),
		[]byte("3"),
		[]byte("40"),
	}

	for _, key := range keys {
		root = Insert(root, key, []byte("dummy_value"))
	}
	expected := [][]byte{
		[]byte("3"),
		[]byte("7"),
		[]byte("10"),
		[]byte("12"),
		[]byte("23"),
		[]byte("31"),
		[]byte("35"),
		[]byte("40"),
	}

	result := InOrder(root)
	if !areByteSlicesEqual(expected, result) {
		t.Fatalf("In-order traversal mismatch. Expected %v, Got %v", expected, result)
	}
}

func TestSearch(t *testing.T) {
	var root *Node

	keys := [][]byte{
		[]byte("10"),
		[]byte("23"),
		[]byte("35"),
		[]byte("7"),
	}

	for _, key := range keys {
		root = Insert(root, key, []byte("value"))
	}

	testCases := []struct {
		key      []byte
		expected bool
	}{
		{[]byte("10"), true},
		{[]byte("23"), true},
		{[]byte("99"), false},
		{[]byte("7"), true},
	}

	for _, tc := range testCases {
		found := Search(root, tc.key)
		if found != tc.expected {
			t.Fatalf("Search failed for key %s. Expected %v, Got %v", tc.key, tc.expected, found)
		}
	}
}

func TestDelete(t *testing.T) {

	keys := [][]byte{
		[]byte("10"),
		[]byte("23"),
		[]byte("35"),
		[]byte("7"),
		[]byte("12"),
		[]byte("15"),
	}

	t.Run("delete leaf", func(t *testing.T) {
		var root *Node
		for _, key := range keys {
			root = Insert(root, key, []byte("value"))
		}
		// Brisanje cvora "7" (nema dece)
		root = Delete(root, []byte("7"))
		expected := [][]byte{
			[]byte("10"),
			[]byte("12"),
			[]byte("15"),
			[]byte("23"),
			[]byte("35"),
		}
		result := InOrder(root)
		if !areByteSlicesEqual(expected, result) {
			t.Fatalf("Delete leaf failed. Expected %v, Got %v", expected, result)
		}
	})

	t.Run("delete node with one child", func(t *testing.T) {
		var root *Node
		for _, key := range keys {
			root = Insert(root, key, []byte("value"))
		}
		// Brisanje "12" koji ima samo jedno dete ("15")
		root = Delete(root, []byte("12"))
		expected := [][]byte{
			[]byte("7"),
			[]byte("10"),
			[]byte("15"),
			[]byte("23"),
			[]byte("35"),
		}
		result := InOrder(root)
		if !areByteSlicesEqual(expected, result) {
			t.Fatalf("Delete node with one child failed. Expected %v, Got %v", expected, result)
		}
	})

	t.Run("delete node with two children", func(t *testing.T) {
		var root *Node
		for _, key := range keys {
			root = Insert(root, key, []byte("value"))
		}
		// Brisanje cvora "23" koji ima dva deteta levo "12" i desno "35"
		root = Delete(root, []byte("23"))
		expected := [][]byte{
			[]byte("7"),
			[]byte("10"),
			[]byte("12"),
			[]byte("15"),
			[]byte("35"),
		}
		result := InOrder(root)
		if !areByteSlicesEqual(expected, result) {
			t.Fatalf("Delete node with two children failed. Expected %v, Got %v", expected, result)
		}
	})
}

func TestTraversal(t *testing.T) {
	var root *Node
	keys := [][]byte{
		[]byte("10"),
		[]byte("23"),
		[]byte("35"),
		[]byte("7"),
		[]byte("12"),
		[]byte("31"),
		[]byte("3"),
		[]byte("40"),
	}

	for _, key := range keys {
		root = Insert(root, key, []byte("dummy_value"))
	}

	// In-order ocekivani
	expectedInOrder := [][]byte{
		[]byte("3"),
		[]byte("7"),
		[]byte("10"),
		[]byte("12"),
		[]byte("23"),
		[]byte("31"),
		[]byte("35"),
		[]byte("40"),
	}

	// Pre-order ocekivani
	expectedPreOrder := [][]byte{
		[]byte("10"),
		[]byte("7"),
		[]byte("3"),
		[]byte("23"),
		[]byte("12"),
		[]byte("35"),
		[]byte("31"),
		[]byte("40"),
	}

	// Post-order ocekiavni
	expectedPostOrder := [][]byte{
		[]byte("3"),
		[]byte("7"),
		[]byte("12"),
		[]byte("31"),
		[]byte("40"),
		[]byte("35"),
		[]byte("23"),
		[]byte("10"),
	}

	// Test in-order obilaska
	inOrderResult := InOrder(root)
	if !areByteSlicesEqual(expectedInOrder, inOrderResult) {
		t.Fatalf("In-order traversal mismatch. Expected %v, Got %v", expectedInOrder, inOrderResult)
	}

	// Test pre-order obilaska
	preOrderResult := PreOrder(root)
	if !areByteSlicesEqual(expectedPreOrder, preOrderResult) {
		t.Fatalf("Pre-order traversal mismatch. Expected %v, Got %v", expectedPreOrder, preOrderResult)
	}

	// Test post-order obilaska
	postOrderResult := PostOrder(root)
	if !areByteSlicesEqual(expectedPostOrder, postOrderResult) {
		t.Fatalf("Post-order traversal mismatch. Expected %v, Got %v", expectedPostOrder, postOrderResult)
	}
}
