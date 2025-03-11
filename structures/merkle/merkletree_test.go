package merkle

import (
	"fmt"
	"testing"
)

func TestNewMerkleTree(t *testing.T) {
	data := [][]byte{
		[]byte("data1"),
		[]byte("data2"),
		[]byte("data3"),
		[]byte("data4"),
	}
	tree := NewMerkleTree(data)
	if tree == nil || tree.Root == nil {
		t.Fatal("Merkle Tree or root is nil")
	}
	fmt.Println("Root hash: ", tree.MerkleRootHash.Hash)
}

func TestSameRootHash(t *testing.T) {
	data := [][]byte{
		[]byte("data1"),
		[]byte("data2"),
	}
	tree1 := NewMerkleTree(data)
	tree2 := NewMerkleTree(data)
	if tree1 == nil || tree2 == nil {
		t.Fatal("Merkle Tree is nil")
	}
	fmt.Println("Root hash 1: ", tree1.MerkleRootHash.Hash)
	fmt.Println("Root hash 2: ", tree2.MerkleRootHash.Hash)
	if tree1.MerkleRootHash.Hash != tree2.MerkleRootHash.Hash {
		t.Fatal("Root hashes are not equal")
	}
}

func TestDifferentRootHash(t *testing.T) {
	data1 := [][]byte{
		[]byte("data1"),
		[]byte("data2"),
	}
	data2 := [][]byte{
		[]byte("data1"),
		[]byte("data3"),
	}
	tree1 := NewMerkleTree(data1)
	tree2 := NewMerkleTree(data2)
	if tree1 == nil || tree2 == nil {
		t.Fatal("Merkle Tree is nil")
	}
	fmt.Println("Root hash 1: ", tree1.MerkleRootHash.Hash)
	fmt.Println("Root hash 2: ", tree2.MerkleRootHash.Hash)
	if tree1.MerkleRootHash.Hash == tree2.MerkleRootHash.Hash {
		t.Fatal("Root hashes are equal")
	}
}

func TestSerializeToBinaryFile(t *testing.T) {
	data := [][]byte{
		[]byte("data1"),
		[]byte("data2"),
	}
	tree := NewMerkleTree(data)
	if tree == nil {
		t.Fatal("Merkle Tree is nil")
	}
	err := tree.SerializeToBinaryFile("merklee.bin")
	if err != nil {
		t.Fatal("Error serializing to binary file")
	}
}

func TestDeserializeFromBinaryFile(t *testing.T) {
	data := [][]byte{
		[]byte("data1"),
		[]byte("data2"),
	}
	tree := NewMerkleTree(data)
	if tree == nil {
		t.Fatal("Merkle Tree is nil")
	}
	err := tree.SerializeToBinaryFile("merklee.bin")
	if err != nil {
		t.Fatal("Error serializing to binary file")
	}
	deserializedTree, err := DeserializeFromBinaryFile("merklee.bin")
	if err != nil {
		t.Fatal("Error deserializing from binary file")
	}
	if tree.MerkleRootHash.Hash != deserializedTree.MerkleRootHash.Hash {
		t.Fatal("Root hashes are not equal")
	}
	fmt.Println("Root hash: ", tree.MerkleRootHash.Hash)
	fmt.Println("Deserialized root hash: ", deserializedTree.MerkleRootHash.Hash)
}
