package binarytree

import (
	"strconv"
)

type Node struct {
	Key   []byte
	Value []byte
	Left  *Node
	Right *Node
}

// Poredi kljuceve numericki
func compareKeys(a, b []byte) int {
	ai, _ := strconv.Atoi(string(a))
	bi, _ := strconv.Atoi(string(b))
	if ai < bi {
		return -1
	} else if ai > bi {
		return 1
	}
	return 0
}

// Dodaje novi covr u binarno stablo, poredeci kljuceve numericki
func Insert(root *Node, key []byte, value []byte) *Node {
	if root == nil {
		return &Node{Key: key, Value: value}
	}

	switch compareKeys(key, root.Key) {
	case -1:
		root.Left = Insert(root.Left, key, value)
	case 1:
		root.Right = Insert(root.Right, key, value)
	default:
		root.Value = value // Azuriaraj vrednost ako kljuc vec postoji
	}

	return root
}

// Pretraga stabla
func Search(root *Node, key []byte) bool {
	if root == nil {
		return false
	}

	switch compareKeys(key, root.Key) {
	case -1:
		return Search(root.Left, key)
	case 1:
		return Search(root.Right, key)
	default:
		return true
	}
}

// Pronalazi najmanji cvor u desnom podstablu
func FindSuccessor(root *Node) *Node {
	for root.Left != nil {
		root = root.Left
	}
	return root
}

// Briše čvor iz stabla
func Delete(root *Node, key []byte) *Node {
	if root == nil {
		return nil
	}

	switch compareKeys(key, root.Key) {
	case -1:
		root.Left = Delete(root.Left, key)
	case 1:
		root.Right = Delete(root.Right, key)
	default:
		// Cvor sa jednim ili nijednim potomkom
		if root.Left == nil {
			return root.Right
		} else if root.Right == nil {
			return root.Left
		}

		// Cvor sa dva potomka
		succ := FindSuccessor(root.Right)
		root.Key = succ.Key
		root.Value = succ.Value
		root.Right = Delete(root.Right, succ.Key)
	}
	return root
}

// InOrder obilazak stabla – vraca slice kljuceva.
func InOrder(root *Node) [][]byte {
	var result [][]byte
	var stack []*Node
	curr := root

	for curr != nil || len(stack) > 0 {
		for curr != nil {
			stack = append(stack, curr)
			curr = curr.Left
		}
		curr = stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		result = append(result, curr.Key)
		curr = curr.Right
	}
	return result
}

// Funkcija koja ispisuje binarno stablo u preorder obilasku
func PreOrder(root *Node) [][]byte {
	var result [][]byte
	var stack []*Node
	curr := root

	for curr != nil || len(stack) > 0 {
		for curr != nil {
			result = append(result, curr.Key)
			stack = append(stack, curr)
			curr = curr.Left
		}
		curr = stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		curr = curr.Right
	}
	return result
}

func PostOrder(root *Node) [][]byte {
	var result [][]byte
	var stack []*Node
	curr := root
	var prev *Node // Da pratimo poslednji dodat cvor

	for curr != nil || len(stack) > 0 {
		for curr != nil {
			stack = append(stack, curr)
			curr = curr.Left
		}
		curr = stack[len(stack)-1]
		// Ako desno dete postoji i nije poseceno predji na njega
		if curr.Right != nil && curr.Right != prev {
			curr = curr.Right
		} else {
			result = append(result, curr.Key)
			stack = stack[:len(stack)-1]
			// Trenutni cvor postaje poslednji dodat cvor
			prev = curr
			// Postavljamo curr na nil da izbegnemo ponovno dodavanje cvora
			curr = nil
		}
	}
	return result
}
