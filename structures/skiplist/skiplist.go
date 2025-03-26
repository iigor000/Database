package skiplist

import (
	"bytes"
	"encoding/binary"
	"math"
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
	key   int
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

func MakeSkipList(maxHeight int) *SkipList {
	root := Node{key: math.MinInt}

	node := &root

	for i := 0; i < maxHeight; i++ {
		newNode := Node{key: node.key}
		node.down = &newNode
		node = &newNode
	}

	return &SkipList{maxHeight: maxHeight, root: &root, size: maxHeight}
}

func (s *SkipList) Update1(key int, value []byte) {
	nodes := s.SearchNodes(key)
	for _, node := range nodes {
		if node.next != nil && node.next.key == key {
			node.next.value = value
		}
	}
}

func (s *SkipList) Search(value int) []byte {
	node := s.root

	for node.key != value {
		if node.next == nil || node.next.key > value {
			if node.down != nil {
				node = node.down
			} else {
				break
			}
		} else {
			node = node.next
		}
	}

	if node.key != value {
		return []byte("")
	}

	return node.value
}

func (s *SkipList) SearchNodes(value int) []*Node {
	node := s.root
	nodes := make([]*Node, 0)
	for node.key != value && node.down != nil {
		if node.next == nil || node.next.key > value {
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

	for node.next != nil && node.next.key <= value {
		node = node.next
	}

	nodes = append(nodes, node)

	return nodes
}

func (s *SkipList) Add(key int, value []byte) {
	levels := s.roll()

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

func (s *SkipList) SearchBeforeNodes(key int) []*Node {
	node := s.root
	nodes := make([]*Node, 0)
	for node.down != nil {
		if node.next == nil || node.next.key >= key {
			nodes = append(nodes, node)
			node = node.down
		} else {
			node = node.next
		}
	}

	for node.next != nil && node.next.key != key {
		node = node.next
	}

	if node.next != nil && node.next.key != key {
		panic("key not found")
	}

	nodes = append(nodes, node)

	return nodes
}

func (s *SkipList) Remove(key int) {
	nodes := s.SearchBeforeNodes(key)

	for _, node := range nodes {
		if node.next != nil && node.next.key == key {
			node.next = node.next.next
		}
		s.size--
	}
}

func (s *SkipList) Create(key int, value []byte, timestamp int64, tombstone bool) {
	entry := memtable.MemtableEntry{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
		Tombstone: tombstone,
	}
	serialized := serializeEntry(entry)
	s.Add(key, serialized)
}

func (s *SkipList) Read(key int) (*memtable.MemtableEntry, bool) {
	value := s.Search(key)
	if value == nil {
		return nil, false
	}
	if len(value) == 0 {
		return nil, false
	}
	entry := deserializeEntry(value)
	if entry.Tombstone {
		return nil, false
	}
	return &entry, true
}

func (s *SkipList) Delete(key int) {
	entry, found := s.Read(key)
	if !found {
		return
	}
	s.Remove(key)
	entry.Tombstone = true
	serialized := serializeEntry(*entry)
	s.Add(key, serialized)

}
func (s *SkipList) Update(key int, value []byte) {
	entry, found := s.Read(key)
	if !found {
		s.Create(key, value, time.Now().UnixNano(), false)
		return
	}
	s.Remove(key)
	entry.Value = value
	entry.Timestamp = time.Now().UnixNano()
	serialized := serializeEntry(*entry)
	s.Add(key, serialized)
}

func serializeEntry(entry memtable.MemtableEntry) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, entry.Key)
	binary.Write(buf, binary.BigEndian, int64(len(entry.Value)))
	buf.Write(entry.Value)
	binary.Write(buf, binary.BigEndian, entry.Timestamp)
	binary.Write(buf, binary.BigEndian, entry.Tombstone)
	return buf.Bytes()
}

func deserializeEntry(data []byte) memtable.MemtableEntry {
	buf := bytes.NewReader(data)
	entry := memtable.MemtableEntry{}
	binary.Read(buf, binary.BigEndian, &entry.Key)
	var valueLen int64
	binary.Read(buf, binary.BigEndian, &valueLen)
	entry.Value = make([]byte, valueLen)
	buf.Read(entry.Value)
	binary.Read(buf, binary.BigEndian, &entry.Timestamp)
	binary.Read(buf, binary.BigEndian, &entry.Tombstone)
	return entry
}

func (s *SkipList) Clear() {
	root := Node{key: math.MinInt}
	node := &root

	for i := 0; i < s.maxHeight; i++ {
		newNode := Node{key: node.key}
		node.down = &newNode
		node = &newNode
	}
	s.root = &root
	s.size = s.maxHeight
}
