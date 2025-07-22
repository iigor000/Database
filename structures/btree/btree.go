package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/iigor000/database/structures/adapter"
	memtable "github.com/iigor000/database/structures/adapter"
)

type BTree struct {
	root *Node
	t    int // minimalni stepen stabla
}

type Node struct {
	keys     [][]byte // kljucevi u cvoru (sada []byte umesto byte)
	values   [][]byte // vrednosti povezane sa kljucevima
	children []*Node  // pokazivaci na decu
	leaf     bool     // da li je list
}

// NewBTree kreira novo B stablo sa zadatim minimalnim stepenom t
func NewBTree(t int) *BTree {
	if t < 2 {
		t = 2 // minimalni stepen ne moze biti manji od 2
	}
	return &BTree{t: t}
}

func (t *BTree) Search(k []byte) (*adapter.MemtableEntry, bool) {
	if t.root == nil {
		return nil, false
	}
	value := t.Search1(k)
	if value == nil {
		return nil, false
	}
	entry := deserializeEntry(value)
	return &entry, true
}

func (t *BTree) Update(k, v []byte, timestamp int64, tombstone bool) {
	if t.root == nil {
		return
	}
	_, exist := t.Search(k)
	entry := memtable.MemtableEntry{
		Key:       k,
		Value:     v,
		Timestamp: timestamp,
		Tombstone: tombstone,
	}
	value := serializeEntry(entry)
	if exist {
		t.update(k, value)
	} else {
		t.Insert(k, value)
	}

}

func (t *BTree) Delete(k []byte) {
	if t.root == nil {
		return
	}
	entry, found := t.Search(k)
	if !found {
		return // kljuc ne postoji
	}
	entry.Tombstone = true // oznacavamo kao obrisano
	value := serializeEntry(*entry)
	t.Delete1(k) // uklanjamo kljuc iz stabla
	t.Insert(k, value)
}

// update azurira vrednost za kljuc k u B stablu
func (t *BTree) update(k, v []byte) {

}

// Search pretrazuje B stablo za kljucem k i vraca odgovarajucu vrednost
func (t *BTree) Search1(k []byte) []byte {
	if t.root == nil {
		return nil
	}
	return search(t.root, k)
}

// search rekurzivno pretrazuje cvor x za kljucem k
func search(x *Node, k []byte) []byte {
	i := 0
	for i < len(x.keys) && bytesCompare(k, x.keys[i]) > 0 {
		i++
	}

	if i < len(x.keys) && bytesEqual(k, x.keys[i]) {
		return x.values[i]
	}

	if x.leaf {
		return nil
	}

	// Provera da li dete postoji pre rekurzivnog poziva
	if i >= len(x.children) {
		return nil
	}
	return search(x.children[i], k)
}

// Insert umetne kljuc k i vrednost v u B stablo
func (t *BTree) Insert(k, v []byte) {
	if t.root == nil {
		// Prvi unos u stablo - korenski cvor
		t.root = &Node{
			keys:     [][]byte{k},
			values:   [][]byte{v},
			children: []*Node{},
			leaf:     true,
		}
		return
	}

	// Ako je koren pun, podeli ga pre umetanja
	if len(t.root.keys) == 2*t.t-1 {
		newRoot := &Node{
			children: []*Node{t.root},
			leaf:     false,
		}
		t.splitChild(newRoot, 0)
		t.root = newRoot
	}
	t.insertNonFull(t.root, k, v)
}

// splitChild deli puno dete i ubacuje srednji kljuc u roditelja x
func (t *BTree) splitChild(x *Node, i int) {
	y := x.children[i]
	z := &Node{leaf: y.leaf}

	mid := t.t - 1 // indeks srednjeg kljuca

	// Sacuvaj srednji kljuc i vrednost
	midKey := y.keys[mid]
	midValue := y.values[mid]

	// Desna polovina kljuceva i vrednosti ide u novi cvor z
	z.keys = append(z.keys, y.keys[mid+1:]...)
	z.values = append(z.values, y.values[mid+1:]...)

	// Leva polovina ostaje u y
	y.keys = y.keys[:mid]
	y.values = y.values[:mid]

	// Ako cvor nije list, podeli i decu
	if !y.leaf {
		z.children = append(z.children, y.children[mid+1:]...)
		y.children = y.children[:mid+1]
	}

	// Ubacivanje srednjeg kljuca i vrednosti u roditelja
	x.keys = append(x.keys, nil)
	copy(x.keys[i+1:], x.keys[i:])
	x.keys[i] = midKey

	x.values = append(x.values, nil)
	copy(x.values[i+1:], x.values[i:])
	x.values[i] = midValue

	// Ubacivanje novog deteta u roditelja
	x.children = append(x.children, nil)
	copy(x.children[i+2:], x.children[i+1:])
	x.children[i+1] = z
}

// insertNonFull umetne kljuc k i vrednost v u cvor x koji nije pun
func (t *BTree) insertNonFull(x *Node, k, v []byte) {
	i := len(x.keys) - 1

	if x.leaf {
		// Ako kljuc vec postoji, samo azuriraj vrednost
		for j := 0; j < len(x.keys); j++ {
			if bytesEqual(x.keys[j], k) {
				x.values[j] = v
				return
			}
		}

		// Ubacivanje novog kljuca i vrednosti u list
		x.keys = append(x.keys, nil)
		x.values = append(x.values, nil)
		for i >= 0 && bytesCompare(k, x.keys[i]) < 0 {
			x.keys[i+1] = x.keys[i]
			x.values[i+1] = x.values[i]
			i--
		}
		x.keys[i+1] = k
		x.values[i+1] = v
	} else {
		// Nadji odgovarajuce dete za umetanje
		for i >= 0 && bytesCompare(k, x.keys[i]) < 0 {
			i--
		}
		i++

		// Ako je dete puno, podeli ga
		if len(x.children[i].keys) == 2*t.t-1 {
			t.splitChild(x, i)
			if bytesCompare(k, x.keys[i]) > 0 {
				i++
			}
		}
		t.insertNonFull(x.children[i], k, v)
	}
}

// Delete uklanja kljuc k iz B stabla
func (t *BTree) Delete1(k []byte) {
	if t.root == nil {
		return
	}

	t.delete(t.root, k)

	// Ako koren ostane prazan, smanji visinu stabla
	if len(t.root.keys) == 0 {
		if t.root.leaf {
			t.root = nil
		} else {
			t.root = t.root.children[0]
		}
	}
}

// delete uklanja kljuc k iz cvor x
func (t *BTree) delete(x *Node, k []byte) {
	i := 0
	for i < len(x.keys) && bytesCompare(x.keys[i], k) < 0 {
		i++
	}

	// Kljuc je u ovom cvoru
	if i < len(x.keys) && bytesEqual(x.keys[i], k) {
		if x.leaf {
			// Brisanje iz lista
			t.deleteFromLeaf(x, i)
		} else {
			// Brisanje iz internog cvora
			t.deleteFromInternalNode(x, i)
		}
	} else {
		if x.leaf {
			// Kljuc nije pronadjen
			return
		}

		// Kljuc je u podstablu x.children[i]
		flag := (i == len(x.keys))

		// Ako dete ima premalo kljuceva, popuni ga
		if len(x.children[i].keys) < t.t {
			t.fill(x, i)
		}

		if flag && i > len(x.keys) {
			t.delete(x.children[i-1], k)
		} else {
			t.delete(x.children[i], k)
		}
	}
}

// deleteFromLeaf uklanja kljuc i vrednost iz lista
func (t *BTree) deleteFromLeaf(x *Node, i int) {
	// Ukloni kljuc i vrednost iz lista
	x.keys = append(x.keys[:i], x.keys[i+1:]...)
	x.values = append(x.values[:i], x.values[i+1:]...)
}

// deleteFromInternalNode uklanja kljuc i vrednost iz unutrasnjeg cvora
func (t *BTree) deleteFromInternalNode(x *Node, i int) {
	k := x.keys[i]

	if len(x.children[i].keys) >= t.t {
		// Uzmi prethodnika
		pred := t.getPredecessor(x.children[i])
		x.keys[i] = pred
		x.values[i] = t.getPredecessorValue(x.children[i])
		t.delete(x.children[i], pred)
	} else if len(x.children[i+1].keys) >= t.t {
		// Uzmi sledbenika
		succ := t.getSuccessor(x.children[i+1])
		x.keys[i] = succ
		x.values[i] = t.getSuccessorValue(x.children[i+1])
		t.delete(x.children[i+1], succ)
	} else {
		// Spoji decu i kljuc iz roditelja
		t.merge(x, i)
		t.delete(x.children[i], k)
	}
}

// getPredecessor vraca najveci kljuc u podstablu x
func (t *BTree) getPredecessor(x *Node) []byte {
	if x.leaf {
		return x.keys[len(x.keys)-1]
	}
	return t.getPredecessor(x.children[len(x.children)-1])
}

// getPredecessorValue vraca vrednost najveceg kljuca u podstablu x
func (t *BTree) getPredecessorValue(x *Node) []byte {
	if x.leaf {
		return x.values[len(x.values)-1]
	}
	return t.getPredecessorValue(x.children[len(x.children)-1])
}

// getSuccessor vraca najmanji kljuc u podstablu x
func (t *BTree) getSuccessor(x *Node) []byte {
	if x.leaf {
		return x.keys[0]
	}
	return t.getSuccessor(x.children[0])
}

// getSuccessorValue vraca vrednost najmanjeg kljuca u podstablu x
func (t *BTree) getSuccessorValue(x *Node) []byte {
	if x.leaf {
		return x.values[0]
	}
	return t.getSuccessorValue(x.children[0])
}

// fill popunjava dete x na indeksu i ako ima premalo kljuceva
func (t *BTree) fill(x *Node, i int) {
	if i != 0 && len(x.children[i-1].keys) >= t.t {
		// Pozajmi od levog brata
		t.borrowFromPrev(x, i)
	} else if i != len(x.keys) && len(x.children[i+1].keys) >= t.t {
		// Pozajmi od desnog brata
		t.borrowFromNext(x, i)
	} else {
		// Spoji dete sa bratom
		if i != len(x.keys) {
			t.merge(x, i)
		} else {
			t.merge(x, i-1)
		}
	}
}

// borrowFromPrev pozajmljuje kljuc i vrednost od levog
func (t *BTree) borrowFromPrev(x *Node, i int) {
	child := x.children[i]
	sibling := x.children[i-1]

	// Pomeri kljuc iz roditelja u dete
	child.keys = append([][]byte{x.keys[i-1]}, child.keys...)
	child.values = append([][]byte{x.values[i-1]}, child.values...)

	// Ako nije list, pomeri dete iz brata u dete
	if !child.leaf {
		child.children = append([]*Node{sibling.children[len(sibling.children)-1]}, child.children...)
		sibling.children = sibling.children[:len(sibling.children)-1]
	}

	// Pomeri kljuc iz brata u roditelja
	x.keys[i-1] = sibling.keys[len(sibling.keys)-1]
	x.values[i-1] = sibling.values[len(sibling.values)-1]

	// Ukloni kljuc iz brata
	sibling.keys = sibling.keys[:len(sibling.keys)-1]
	sibling.values = sibling.values[:len(sibling.values)-1]
}

// borrowFromNext pozajmljuje kljuc i vrednost od desnog brata
func (t *BTree) borrowFromNext(x *Node, i int) {
	child := x.children[i]
	sibling := x.children[i+1]

	// Pomeri kljuc iz roditelja u dete
	child.keys = append(child.keys, x.keys[i])
	child.values = append(child.values, x.values[i])

	// Ako nije list, pomeri dete iz brata u dete
	if !child.leaf {
		child.children = append(child.children, sibling.children[0])
		sibling.children = sibling.children[1:]
	}

	// Pomeri kljuc iz brata u roditelja
	x.keys[i] = sibling.keys[0]
	x.values[i] = sibling.values[0]

	// Ukloni kljuc iz brata
	sibling.keys = sibling.keys[1:]
	sibling.values = sibling.values[1:]
}

// merge spaja dete x na indeksu i sa njegovim bratom
func (t *BTree) merge(x *Node, i int) {
	child := x.children[i]
	sibling := x.children[i+1]

	// Dodaj kljuc iz roditelja u dete
	child.keys = append(child.keys, x.keys[i])
	child.values = append(child.values, x.values[i])

	// Dodaj kljuceve i vrednosti iz brata u dete
	child.keys = append(child.keys, sibling.keys...)
	child.values = append(child.values, sibling.values...)

	// Dodaj decu iz brata ako nisu listovi
	if !child.leaf {
		child.children = append(child.children, sibling.children...)
	}

	// Ukloni kljuc i dete iz roditelja
	x.keys = append(x.keys[:i], x.keys[i+1:]...)
	x.values = append(x.values[:i], x.values[i+1:]...)
	x.children = append(x.children[:i+1], x.children[i+2:]...)
}

// bytesCompare poredi dva []byte niza
func bytesCompare(a, b []byte) int {
	return bytes.Compare(a, b)
}

// bytesEqual proverava da li su dva []byte niza jednaka
func bytesEqual(a, b []byte) bool {
	return bytes.Equal(a, b)
}

// Traverse obilazi B stablo i ispisuje kljuceve
func (t *BTree) Traverse() {
	traverse(t.root)
}

// traverse rekurzivno obilazi cvor x i ispisuje njegove kljuceve
func traverse(x *Node) {
	if x != nil {
		for i := 0; i < len(x.keys); i++ {
			if !x.leaf {
				traverse(x.children[i])
			}
			fmt.Printf("%s ", x.keys[i])
		}
		if !x.leaf {
			traverse(x.children[len(x.keys)])
		}
	}
}

// MinToMaxTraversal obilazi B stablo i ispisuje kljuceve od najmanjeg do najveceg
func (t *BTree) MinToMaxTraversal() {
	fmt.Println("B-tree values from smallest to largest:")
	sorted := t.SortedKeys()
	for _, k := range sorted {
		fmt.Printf("%s ", k)
	}
	fmt.Println()
}

// SortedKeys vraca kljuceve B stabla u sortiranom redosledu
func (t *BTree) SortedKeys() [][]byte {
	return collectSortedKeys(t.root)
}

// collectSortedKeys rekurzivno prikuplja kljuceve iz stabla u sortiranom redosledu
func collectSortedKeys(x *Node) [][]byte {
	if x == nil {
		return [][]byte{}
	}

	result := [][]byte{}

	for i := 0; i < len(x.keys); i++ {
		if !x.leaf {
			result = append(result, collectSortedKeys(x.children[i])...)
		}
		result = append(result, x.keys[i])
	}
	if !x.leaf {
		result = append(result, collectSortedKeys(x.children[len(x.keys)])...)
	}

	return result
}

func (t *BTree) Clear() {
	t.root = nil
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
