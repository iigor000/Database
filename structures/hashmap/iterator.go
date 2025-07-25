package hashmap

import (
	"bytes"
	"errors"
	"sort"

	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/util"
)

type Iterator struct {
	keys     []string
	index    int
	hashMap  *HashMap
	maxIndex int
}

type RangeIterator struct {
	Iterator
	startKey []byte
	endKey   []byte
}

type PrefixIterator struct {
	Iterator
	prefix []byte
}

// Pravimo novi iterator, uzimamo sve kljuceve i sortiramo ih
func (hm *HashMap) NewIterator() (*Iterator, error) {
	if len(hm.data) == 0 {
		return nil, errors.New("error: hashmap is empty")
	}

	keys := make([]string, 0)
	for k := range hm.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	iter := &Iterator{
		keys:     keys,
		hashMap:  hm,
		index:    -1, // Start before first element
		maxIndex: len(keys),
	}

	return iter, nil
}

// Prebacujemo iterator na sledeci cvor, ako nije prazan, ili ako je kljuc rezervisan idemo na sledeci
func (h *Iterator) Next() bool {
	h.index++

	// Skip reserved keys
	for h.index < h.maxIndex && util.CheckKeyReserved(h.keys[h.index]) {
		h.index++
	}

	return h.index < h.maxIndex
}

// Vraca trenutni zapis iteratora
func (h *Iterator) Value() *adapter.MemtableEntry {
	if h.index >= 0 && h.index < h.maxIndex {
		value, found := h.hashMap.Search([]byte(h.keys[h.index]))
		if !found {
			return nil
		}
		return value
	}
	return nil
}

// Inicijalizuje iterator koji vraca samo zapise u datom opsegu
func (hm *HashMap) NewRangeIterator(startKey []byte, endKey []byte) (*RangeIterator, error) {
	if len(hm.data) == 0 {
		return nil, errors.New("error: hashmap is empty")
	}

	keys := make([]string, 0)
	for k := range hm.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Nalazimo pocetni indeks
	startIndex := -1
	for i, key := range keys {
		if !util.CheckKeyReserved(key) && bytes.Compare([]byte(key), startKey) >= 0 {
			startIndex = i - 1
			break
		}
	}

	if startIndex == -1 && len(keys) > 0 {
		// Ako ne pronadjemo startIndex, postavljamo ga na duzinu keys
		startIndex = len(keys)
	}

	iter := &Iterator{
		keys:     keys,
		hashMap:  hm,
		index:    startIndex,
		maxIndex: len(keys),
	}

	return &RangeIterator{
		Iterator: *iter,
		startKey: startKey,
		endKey:   endKey,
	}, nil
}

// Prolazi kroz iterator i vraca samo one zapise koji su u opsegu startKey i endKey
func (iter *RangeIterator) Next() bool {
	if !iter.Iterator.Next() {
		return false
	}

	currentValue := iter.Value()
	if currentValue != nil && bytes.Compare(currentValue.Key, iter.endKey) > 0 {
		return false
	}

	return true
}

// Vraca trenutni zapis iteratora
func (iter *RangeIterator) Value() *adapter.MemtableEntry {
	return iter.Iterator.Value()
}

// Inicijalizuje iterator koji vraca samo zapise sa datim prefiksom
func (hm *HashMap) NewPrefixIterator(prefix []byte) (*PrefixIterator, error) {
	iter, err := hm.NewIterator()
	if err != nil {
		return nil, err
	}

	if !iter.Next() {
		return nil, errors.New("error: iterator is empty")
	}

	for iter.Value() != nil && !bytes.HasPrefix(iter.Value().Key, prefix) {
		if !iter.Next() {
			return nil, errors.New("error: could not find prefix")
		}
	}

	if iter.Value() == nil {
		return nil, errors.New("error: could not find prefix")
	}

	return &PrefixIterator{
		Iterator: *iter,
		prefix:   prefix,
	}, nil
}

// Prolazi kroz iterator i vraca samo one zapise koji imaju dati prefiks
func (iter *PrefixIterator) Next() bool {
	if !iter.Iterator.Next() {
		return false
	}

	currentValue := iter.Value()
	if currentValue == nil || !bytes.HasPrefix(currentValue.Key, iter.prefix) {
		return false
	}

	return true
}

// Vraca trenutni zapis iteratora
func (iter *PrefixIterator) Value() *adapter.MemtableEntry {
	return iter.Iterator.Value()
}
