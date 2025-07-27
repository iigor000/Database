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
	value    adapter.MemtableEntry
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
	value, found := hm.Search([]byte(keys[0]))
	if !found {
		return nil, errors.New("error: first key not found in hashmap")
	}

	iter := &Iterator{
		keys:     keys,
		hashMap:  hm,
		index:    0, // Start before first element
		maxIndex: len(keys),
		value:    *value,
	}

	return iter, nil
}

// Prebacujemo iterator na sledeci cvor, ako nije prazan, ili ako je kljuc rezervisan idemo na sledeci
func (h *Iterator) Next() (adapter.MemtableEntry, bool) {
	if h.index >= h.maxIndex {
		h.Stop()
		return h.value, false
	}

	oldValue := h.value

	h.index++

	// Skip reserved keys
	for h.index < h.maxIndex && util.CheckKeyReserved(h.keys[h.index]) {
		h.index++
	}

	if h.index >= 0 && h.index < h.maxIndex {
		value, found := h.hashMap.Search([]byte(h.keys[h.index]))
		if found {
			h.value = *value
		}
	}

	return oldValue, true
}

func (h *Iterator) Stop() {
	h.index = h.maxIndex              // Postavljamo index na maxIndex da bi se iterator zaustavio
	h.value = adapter.MemtableEntry{} // Resetujemo vrednost
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
			startIndex = i
			break
		}
	}

	if startIndex == -1 && len(keys) > 0 {
		// Ako ne pronadjemo startIndex, postavljamo ga na duzinu keys
		startIndex = len(keys)
	}

	value, found := hm.Search([]byte(keys[startIndex]))
	if !found {
		return nil, errors.New("error: start key not found in hashmap")
	}

	iter := &Iterator{
		keys:     keys,
		hashMap:  hm,
		index:    startIndex,
		maxIndex: len(keys),
		value:    *value,
	}

	return &RangeIterator{
		Iterator: *iter,
		startKey: startKey,
		endKey:   endKey,
	}, nil
}

// Prolazi kroz iterator i vraca samo one zapise koji su u opsegu startKey i endKey
func (iter *RangeIterator) Next() (adapter.MemtableEntry, bool) {
	oldValue := iter.value

	value, ok := iter.Iterator.Next()
	if !ok {
		return adapter.MemtableEntry{}, false
	}

	if bytes.Compare(value.Key, iter.endKey) > 0 {
		iter.Stop()
		return adapter.MemtableEntry{}, false
	}

	return oldValue, true
}

// Inicijalizuje iterator koji vraca samo zapise sa datim prefiksom
func (hm *HashMap) NewPrefixIterator(prefix []byte) (*PrefixIterator, error) {
	if len(hm.data) == 0 {
		return nil, errors.New("error: hashmap is empty")
	}

	keys := make([]string, 0)
	for k := range hm.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	startIndex := -1
	for i, key := range keys {
		if !util.CheckKeyReserved(key) && bytes.HasPrefix([]byte(key), prefix) {
			startIndex = i
			break
		}
	}

	if startIndex == -1 {
		return nil, errors.New("error: could not find prefix")
	}

	value, found := hm.Search([]byte(keys[startIndex]))
	if !found {
		return nil, errors.New("error: prefix not found in hashmap")
	}

	iter := &Iterator{
		keys:     keys,
		hashMap:  hm,
		index:    startIndex,
		maxIndex: len(keys),
		value:    *value,
	}

	return &PrefixIterator{
		Iterator: *iter,
		prefix:   prefix,
	}, nil
}

// Prolazi kroz iterator i vraca samo one zapise koji imaju dati prefiks
func (iter *PrefixIterator) Next() (adapter.MemtableEntry, bool) {
	oldValue := iter.value

	value, ok := iter.Iterator.Next()
	if !ok {
		return adapter.MemtableEntry{}, false
	}

	if !bytes.HasPrefix(value.Key, iter.prefix) {
		iter.Stop()
		return adapter.MemtableEntry{}, false
	}

	return oldValue, true
}
