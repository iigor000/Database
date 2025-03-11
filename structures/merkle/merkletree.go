package merkle

import (
	"crypto/sha256"
	"encoding/gob"
	"errors"
	"os"
)

type HashValue struct {
	Hash [32]byte
}

// Funkcija koja racuna hash vrednost podataka
func hashFunc(data [][]byte) []HashValue {
	hashvalues := []HashValue{}
	for _, d := range data {
		hashval := sha256.Sum256(d)
		hashvalues = append(hashvalues, HashValue{Hash: hashval})
	}
	return hashvalues
}

// Cvor struktura koja predstavlja cvor u Merkle stablu, gde je Hash hash vrednost cvora, Left levo dete cvora, Right desno dete cvora
type Node struct {
	Hash  HashValue
	Left  *Node
	Right *Node
}

// MerkleTree struktura koja predtsavlja Merkle stablo, gde je Root koren stabla, MerkleRootHash hash vrednost korena stabla
type MerkleTree struct {
	Root           *Node
	MerkleRootHash HashValue
}

// Kreiramo Merkle stablo od hash vrednosti podataka
func buildMerkleTree(hashes []HashValue) *Node {
	nodes := []Node{}
	// Kreiramo listove od hash vrednosti
	for _, hash := range hashes {
		nodes = append(nodes, Node{Hash: hash})
	}
	// Kreiramo cvorove stabla
	for len(nodes) > 1 {
		var nextLevel []Node
		// Provera da li je neparan broj cvorova, ako jeste dodajemo prazan cvor
		if len(nodes)%2 != 0 {
			emptyhash := hashFunc([][]byte{[]byte{}})
			nodes = append(nodes, Node{Hash: emptyhash[0]})
		}
		for i := 0; i < len(nodes); i += 2 {
			// Racunamo hash vrednost novog cvora
			combhash := make([]byte, 64)
			copy(combhash[32:], nodes[i].Hash.Hash[:])
			copy(combhash[:32], nodes[i+1].Hash.Hash[:])
			newhash := hashFunc([][]byte{combhash})
			newnode := Node{Hash: newhash[0], Left: &nodes[i], Right: &nodes[i+1]}
			// Dodajemo novi cvor u sledeci nivo
			nextLevel = append(nextLevel, newnode)
		}
		nodes = nextLevel

	}
	// Vracamo koren
	return &nodes[0]
}

// Kreira novo Merkle stablo od datih podataka ( gde je podatak konvertovan u niz bajtova )
func NewMerkleTree(data [][]byte) *MerkleTree {
	// Hash vrednosti podataka
	hashes := hashFunc(data)
	// Kreiramo Merkle stablo od hash vrednosti podataka
	root := buildMerkleTree(hashes)
	// Vracamo novo Merkle stablo
	return &MerkleTree{Root: root, MerkleRootHash: root.Hash}
}

// Serijalizacija Merkle stabla u binarnu datoteku merklee.bin
func (t *MerkleTree) SerializeToBinaryFile(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	serializedNodes := BFS(t)
	encoder := gob.NewEncoder(file)
	return encoder.Encode(serializedNodes)
}

// Deserijalizacija Merkle stabla iz binarne datoteke merklee.bin
func DeserializeFromBinaryFile(filename string) (*MerkleTree, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var serializedNodes []Node
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&serializedNodes); err != nil {
		return nil, err
	}

	if len(serializedNodes) == 0 {
		return nil, errors.New("invalid data")
	}
	nodes := make(map[int]*Node)
	for i := range serializedNodes {
		nodes[i] = &serializedNodes[i]
	}
	for i := range serializedNodes {
		if serializedNodes[i].Left != nil {
			leftIndex := (i * 2) + 1
			if leftIndex < len(serializedNodes) {
				nodes[i].Left = nodes[leftIndex]
			}
		}
		if serializedNodes[i].Right != nil {
			rightIndex := (i * 2) + 2
			if rightIndex < len(serializedNodes) {
				nodes[i].Right = nodes[rightIndex]
			}
		}
	}
	root := nodes[0]
	merkleRootHash := root.Hash

	return &MerkleTree{Root: root, MerkleRootHash: merkleRootHash}, nil
}

// Prolazak kroz Merkle stablo u sirinu, nivo po nivo
func BFS(merkletree *MerkleTree) []Node {
	nodes := []*Node{merkletree.Root}
	var queue []Node

	for len(nodes) > 0 {
		n := nodes[0]
		nodes = nodes[1:]
		queue = append(queue, *n)
		if n.Left != nil {
			nodes = append(nodes, n.Left)
		}
		if n.Right != nil {
			nodes = append(nodes, n.Right)
		}
	}
	return queue
}
