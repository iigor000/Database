package skiplist

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"time"

	memtable "github.com/iigor000/database/structures/adapter"
)

type SkipList struct {
	maxHeight int
	root      *Node
	size      int
}

type Node struct {
	next  *Node
	key   []byte
	value []byte
	down  *Node
}

func (s *SkipList) roll() int {
	level := 0
	// possible ret values from rand are 0 and 1
	// we stop shen we get a 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= s.maxHeight {
			return level
		}
	}
	return level
}

// Pravi novu skip listu
func MakeSkipList(maxHeight int) *SkipList {
	root := Node{}

	node := &root

	// Kreiramo donje nivoe
	for i := 0; i < maxHeight; i++ {
		newNode := Node{key: node.key}
		node.down = &newNode
		node = &newNode
	}

	return &SkipList{maxHeight: maxHeight, root: &root, size: 0}
}

// Pretrazuje skip listu
func (s *SkipList) search(value []byte) []byte {
	node := s.root

	// Gledamo da li je kljuc jednak trenutnom, ako nije gledamo sledeci
	// Ako je sledeci veci od trazenog, idemo na donji nivo
	// Ako je sledeci manji, idemo na sledeci
	for !bytes.Equal(node.key, value) {
		if node.next == nil || bytes.Compare(node.next.key, value) == 1 {
			if node.down != nil {
				node = node.down
			} else {
				break
			}
		} else {
			node = node.next
		}
	}

	if !bytes.Equal(node.key, value) {
		return []byte("")
	}

	return node.value
}

// Pretrazuje skip listu
func (s *SkipList) SearchNodes(value []byte) []*Node {
	node := s.root
	nodes := make([]*Node, 0)

	// Pronalazimo sve cvorove koji su manji od trazenog
	for !bytes.Equal(node.key, value) && node.down != nil {
		if node.next == nil || bytes.Compare(node.next.key, value) == 1 {
			nodes = append(nodes, node)
			node = node.down
		} else {
			node = node.next
		}
	}

	for node.down != nil {
		nodes = append(nodes, node)
		node = node.down
	}

	for node.next != nil && bytes.Compare(node.next.key, value) != 1 {
		node = node.next
	}

	nodes = append(nodes, node)

	return nodes
}

// Dodaje novi cvor u skip listu
func (s *SkipList) Add(key []byte, value []byte) {
	levels := s.roll()

	// Trazimo mesto za cvor
	nodes := s.SearchNodes(key)
	newNodeDown := &Node{key: key, value: value, next: nodes[len(nodes)-1].next}
	nodes[len(nodes)-1].next = newNodeDown
	s.size++

	for i := 0; i < levels; i++ {
		newNode := Node{key: key, value: value, next: nodes[len(nodes)-i-2].next, down: newNodeDown}
		nodes[len(nodes)-i-2].next = &newNode
		newNodeDown = &newNode
		s.size++
	}
}

// Nalazimo prethodne cvorove (koristi se za delete)
func (s *SkipList) SearchBeforeNodes(key []byte) []*Node {
	node := s.root
	nodes := make([]*Node, 0)
	for node.down != nil {
		if node.next == nil || bytes.Compare(node.next.key, key) != -1 {
			nodes = append(nodes, node)
			node = node.down
		} else {
			node = node.next
		}
	}

	for node.next != nil && !bytes.Equal(node.next.key, key) {
		node = node.next
	}

	if node.next != nil && !bytes.Equal(node.next.key, key) {
		panic("key not found")
	}

	nodes = append(nodes, node)

	return nodes
}

func (s *SkipList) Remove(key []byte) {
	nodes := s.SearchBeforeNodes(key)

	for _, node := range nodes {
		if node.next != nil && bytes.Equal(node.next.key, key) {
			node.next = node.next.next
		}
		s.size--
	}
}

// Kreira novi cvor (za koriscenje u memtablu)
func (s *SkipList) Create(key []byte, value []byte, timestamp int64, tombstone bool) {
	entry := memtable.MemtableEntry{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
		Tombstone: tombstone,
	}
	serialized := serializeEntry(entry)
	s.Add(key, serialized)
	s.size++
}

// Trazi novi cvor (za memtable)
func (s *SkipList) Search(key []byte) (*memtable.MemtableEntry, bool) {
	value := s.search(key)

	if value == nil {
		return nil, false
	}
	if len(value) == 0 {
		return nil, false
	}
	entry := deserializeEntry(value)

	return &entry, true
}

// Brise cvor (za memtable)
func (s *SkipList) Delete(key []byte) {
	entry, found := s.Search(key)
	if !found {
		return
	}
	s.Remove(key)
	entry.Tombstone = true
	serialized := serializeEntry(*entry)
	s.Add(key, serialized)

}

// Azurira cvor - ako ne postoji doda ga, a ako postoji menja vrednost (za memtable)
func (s *SkipList) Update(key []byte, value []byte, timestamp int64, tombstone bool) {
	entry, found := s.Search(key)
	if !found {
		s.Create(key, value, time.Now().UnixNano(), tombstone)
		return
	}
	s.Remove(key)
	entry.Value = value
	entry.Timestamp = timestamp
	entry.Tombstone = tombstone
	serialized := serializeEntry(*entry)
	s.Add(key, serialized)
}

func serializeEntry(entry memtable.MemtableEntry) []byte {
	buf := new(bytes.Buffer)
	var keyLen int64 = int64(len(entry.Key))
	binary.Write(buf, binary.BigEndian, keyLen)
	buf.Write(entry.Key)
	var valueLen int64 = int64(len(entry.Value))
	binary.Write(buf, binary.BigEndian, valueLen)
	buf.Write(entry.Value)
	binary.Write(buf, binary.BigEndian, entry.Timestamp)
	binary.Write(buf, binary.BigEndian, entry.Tombstone)
	return buf.Bytes()
}

func deserializeEntry(data []byte) memtable.MemtableEntry {
	buf := bytes.NewReader(data)
	var keyLen int64
	binary.Read(buf, binary.BigEndian, &keyLen)
	key := make([]byte, keyLen)
	binary.Read(buf, binary.BigEndian, &key)
	var valueLen int64
	binary.Read(buf, binary.BigEndian, &valueLen)
	value := make([]byte, valueLen)
	binary.Read(buf, binary.BigEndian, &value)
	var timestamp int64
	binary.Read(buf, binary.BigEndian, &timestamp)
	var tombstone bool
	binary.Read(buf, binary.BigEndian, &tombstone)
	return memtable.MemtableEntry{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
		Tombstone: tombstone,
	}
}

// Clear - briše sve čvorove u skip listi
func (s *SkipList) Clear() {
	root := Node{}
	node := &root

	for i := 0; i < s.maxHeight; i++ {
		newNode := Node{key: node.key}
		node.down = &newNode
		node = &newNode
	}
	s.root = &root
	s.size = s.maxHeight
}

func (s *SkipList) isEmpty() bool {
	return s.size == 0
}
