package btree

import (
	"bytes"
	"errors"
)

// Iterator omogucava sekvencijalni pristup elementima B-stabla
type Iterator struct {
	keys     [][]byte
	values   [][]byte
	index    int
	maxIndex int
	tree     *BTree
}

// RangeIterator omogucava iteraciju preko kljuceva u odredjenom opsegu
type RangeIterator struct {
	Iterator
	startKey []byte
	endKey   []byte
}

// PrefixIterator omogucava iteraciju preko kljuceva koji imaju odredjeni prefiks
type PrefixIterator struct {
	Iterator
	prefix []byte
}

// 1. NewIterator
// Algoritam:
//   - Prikuplja sve kljuceve i vrednosti iz B-stabla sortirane po redosledu (in-order traversal)
//   - Cuva ih u nizovima unutar iteratora
//   - Pamtimo indeks za trenutnu poziciju
//
// Slozenost:
//   - Inicijalizacija: O(n) po vremenu i prostoru (prikuplja sve elemente)
//   - Next(): O(1)
//   - Value(): O(1)

// NewIterator kreira novi iterator za celokupno B-stablo
func (t *BTree) NewIterator() (*Iterator, error) {
	if t.root == nil {
		return nil, errors.New("error: B-tree is empty")
	}

	// prikupiti sve kljuceve i vrednosti u sortiranom redosledu
	keys, values := collectSortedKeysAndValues(t.root)

	return &Iterator{
		keys:     keys,
		values:   values,
		index:    -1, // pocetak iteratora je pre prvog elementa
		maxIndex: len(keys) - 1,
		tree:     t,
	}, nil
}

// Pomera iterator na sledecu poziciju
func (it *Iterator) Next() bool {
	it.index++
	return it.index <= it.maxIndex
}

// Vraca trenutni kljuc i vrednost iteratora
func (it *Iterator) Value() ([]byte, []byte) {
	if it.index >= 0 && it.index <= it.maxIndex {
		return it.keys[it.index], it.values[it.index]
	}
	return nil, nil
}

// 2. NewRangeIterator
// Algoritam:
//   - Prikuplja sve kljuceve i vrednosti sortirane po redosledu (isto kao gorepoomenuti iterator)
//   - Pronalazi prvi kljuc koji je >= startKey koristeci binarnu pretragu
//   - Iterira dok ne dođe do kljuca koji je > endKey
//
// Slozenost:
//   - Inicijalizacija: O(n) po vremenu i prostoru (prikupljanje svih elemenata)
//   - Next(): O(1) u prosecnom slucaju (moze preskociti neke elemente)

// kreira iteraator za odredjeni opseg kljuceva
func (t *BTree) NewRangeIterator(startKey, endKey []byte) (*RangeIterator, error) {
	if t.root == nil {
		return nil, errors.New("error: B-tree is empty")
	}

	keys, values := collectSortedKeysAndValues(t.root)

	// nadji prvi kljuc koji je veci ili jednak startKey
	startIndex := -1
	for i, key := range keys {
		if bytes.Compare(key, startKey) >= 0 {
			startIndex = i - 1
			break
		}
	}

	if startIndex == -1 && len(keys) > 0 {
		// ako nije pronadjen kljuc veci ili jednak startKey, idi na kraj
		startIndex = len(keys)
	}

	iter := &Iterator{
		keys:     keys,
		values:   values,
		index:    startIndex,
		maxIndex: len(keys) - 1,
		tree:     t,
	}

	return &RangeIterator{
		Iterator: *iter,
		startKey: startKey,
		endKey:   endKey,
	}, nil
}

// Pomera range iterator na sledecu poziciju u okviru opsega
func (rit *RangeIterator) Next() bool {
	if !rit.Iterator.Next() {
		return false
	}

	// proverimo da je trenutni kljuc unutar opsega
	if rit.index > rit.maxIndex || bytes.Compare(rit.keys[rit.index], rit.endKey) > 0 {
		return false
	}

	return true
}

// 3.nNewPrefixIterator
// Algoritam:
//   - Krece od NewIterator iteratora
//   - Pronalazi prvi kljuc sa trayenim prefiksom
//   - Nastavlja iteraciju dok kljucevi imaju dati prefiks
//
// Slozenost:
//   - Inicijalizacija: O(n) u najgorem slucaju (ako prefiks ne postoji)
//   - Next(): O(1) u prosecnom slucaju

// Kreira prefix iterator koji omogucava iteraciju preko kljuceva koji imaju odredjeni prefiks
func (t *BTree) NewPrefixIterator(prefix []byte) (*PrefixIterator, error) {
	iter, err := t.NewIterator()
	if err != nil {
		return nil, err
	}

	// nadji prvi kljuc koji ima dati prefiks
	for iter.Next() {
		key, _ := iter.Value()
		if bytes.HasPrefix(key, prefix) {
			// vrati iterator na prethodni element da bi Next() vratio ovaj element prvi
			iter.index--
			break
		}
	}

	if iter.index >= iter.maxIndex {
		return nil, errors.New("error: prefix not found")
	}

	return &PrefixIterator{
		Iterator: *iter,
		prefix:   prefix,
	}, nil
}

// Next pomera prefix iterator na sledecu poziciju koja ima odgovarajuci prefiks
func (pit *PrefixIterator) Next() bool {
	if !pit.Iterator.Next() {
		return false
	}

	// da li je trenutni kljuc tog prefiksa
	if pit.index > pit.maxIndex || !bytes.HasPrefix(pit.keys[pit.index], pit.prefix) {
		return false
	}

	return true
}

// rekurzivna funkcija koja prikuplja sve kljuceve i vrednosti u sortiranom redosledu
func collectSortedKeysAndValues(x *Node) ([][]byte, [][]byte) {
	if x == nil {
		return [][]byte{}, [][]byte{}
	}

	keys := make([][]byte, 0)
	values := make([][]byte, 0)

	for i := 0; i < len(x.keys); i++ {
		if !x.leaf {
			childKeys, childValues := collectSortedKeysAndValues(x.children[i])
			keys = append(keys, childKeys...)
			values = append(values, childValues...)
		}
		keys = append(keys, x.keys[i])
		values = append(values, x.values[i])
	}
	if !x.leaf {
		childKeys, childValues := collectSortedKeysAndValues(x.children[len(x.keys)])
		keys = append(keys, childKeys...)
		values = append(values, childValues...)
	}

	return keys, values
}
