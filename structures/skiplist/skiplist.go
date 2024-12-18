package skiplist

import (
	"math"
	"math/rand"
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

func MakeSkipList(maxHeight int) SkipList {
	root := Node{key: math.MinInt}

	node := &root

	for i := 0; i < maxHeight; i++ {
		newNode := Node{key: node.key}
		node.down = &newNode
		node = &newNode
	}

	return SkipList{maxHeight: maxHeight, root: &root, size: maxHeight}
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
