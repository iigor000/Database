package merkle

import (
	"fmt"
	"testing"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/block_organization"
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
	_, err := tree.SerializeToBinaryFile("merklee.bin", 0)
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
	_, err := tree.SerializeToBinaryFile("merklee.bin", 0)
	if err != nil {
		t.Fatal("Error serializing to binary file")
	}
	deserializedTree, err := DeserializeFromBinaryFile("merklee.bin", 0)
	if err != nil {
		t.Fatal("Error deserializing from binary file")
	}
	if tree.MerkleRootHash.Hash != deserializedTree.MerkleRootHash.Hash {
		t.Fatal("Root hashes are not equal")
	}
	fmt.Println("Root hash: ", tree.MerkleRootHash.Hash)
	fmt.Println("Deserialized root hash: ", deserializedTree.MerkleRootHash.Hash)
}

func TestWriteReadToFile(t *testing.T) {
	conf := &config.Config{
		Block: config.BlockConfig{
			BlockSize: 4096,
		},
		Cache: config.CacheConfig{
			Capacity: 100,
		},
	}

	data := [][]byte{
		[]byte("data1"),
		[]byte("data2"),
	}
	tree := NewMerkleTree(data)
	if tree == nil {
		t.Fatal("Merkle Tree is nil")
	}
	bytesToWrite, err := tree.Serialize()
	if err != nil {
		t.Fatal("Error serializing Merkle Tree")
	}
	bm := block_organization.NewBlockManager(conf)
	bc := block_organization.NewBlockCache(conf)
	cbm := &block_organization.CachedBlockManager{
		BM: bm,
		C:  bc,
	}

	bn, err := cbm.Append("merkletree.db", bytesToWrite)
	if err != nil {
		t.Fatal("Error writing to block manager")
	}
	println("Merkle Tree written to file successfully")
	block, err := cbm.Read("merkletree.db", bn)
	if err != nil {
		t.Fatal("Error reading from block manager")
	}
	println("Read", len(block), "bytes from file merkletree.db")
	newTree, err := Deserialize(block)
	if err != nil {
		t.Fatal("Error deserializing block")
	}
	if tree.MerkleRootHash.Hash != newTree.MerkleRootHash.Hash {
		t.Fatal("Root hashes are not equal")
	}
}

func TestCompareMerkleTrees(t *testing.T) {
	data1 := [][]byte{
		[]byte("data1"),
		[]byte("data2"),
		[]byte("data3"),
		[]byte("data4"),
	}
	data2 := [][]byte{
		[]byte("data1"),
		[]byte("data5"),
		[]byte("data3"),
		[]byte("data6"),
	}
	tree1 := NewMerkleTree(data1)
	tree2 := NewMerkleTree(data2)
	if tree1 == nil || tree2 == nil {
		t.Fatal("Merkle Tree is nil")
	}
	differences := tree1.Compare(tree2)
	if len(differences) == 0 {
		t.Fatal("Merkle Trees are identical")
	}
	fmt.Println("Differences found at indices: ", differences)
}
