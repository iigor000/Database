package skiplist

import (
	"bytes"
	"errors"

	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/util"
)

type Iterator struct {
	current *Node
}

type RangeIterator struct {
	startKey []byte
	endKey   []byte
	Iterator
}

type PrefixIterator struct {
	prefix []byte
	Iterator
}

// Pravi se novi iterator, uzimamo root i spustamo se na najdonji nivo
func (s *SkipList) NewIterator() (*Iterator, error) {
	if s.isEmpty() {
		return nil, errors.New("skiplist is empty")
	}

	current := s.root
	for current.down != nil {
		current = current.down
	}

	return &Iterator{current: current}, nil
}

// Prebacujemo iterator na sledeci cvor, ako nije prazan, ili ako je kljuc rezervisan idemo na sledeci
func (iter *Iterator) Next() bool {
	for iter.current.next != nil {
		iter.current = iter.current.next
		if !util.CheckKeyReserved(string(iter.current.key)) {
			return true
		}
	}
	iter.current = nil
	return false
}

// Vraca trenutni zapis iteratora
func (iter *Iterator) Value() *adapter.MemtableEntry {
	if iter.current != nil {
		entry := deserializeEntry(iter.current.value)
		return &entry
	}
	return nil
}

// Inicijalizuje iterator koji vraca samo zapise sa datim opsegom kljuceva
func (sl *SkipList) NewRangeIterator(startKey []byte, endKey []byte) (*RangeIterator, error) {
	iter, err := sl.NewIterator()
	if err != nil {
		return nil, err
	}

	if iter.Value() == nil {
		return nil, errors.New("error: could not find startKey")
	}

	// Postavljamo iterator na prvi kljuc koji je veci ili jednak startKey
	for bytes.Compare(iter.Value().Key, startKey) < 0 {
		if !iter.Next() {
			return nil, errors.New("error: could not find startKey")
		}
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
	if bytes.Compare(iter.Value().Key, iter.endKey) > 0 {
		iter.Iterator.current = nil
		return false
	}
	return true
}

// Vraca trenutni zapis iteratora
func (iter *RangeIterator) Value() *adapter.MemtableEntry {
	return iter.Iterator.Value()
}

// Inicijalizuje iterator koji vraca samo zapise sa datim prefiksom
func (sl *SkipList) NewPrefixIterator(prefix []byte) (*PrefixIterator, error) {
	iter, err := sl.NewIterator()
	if err != nil {
		return nil, err
	}

	if iter.Value() == nil {
		return nil, errors.New("error: could not find prefix")
	}

	// Postavljamo iterator na prvi kljuc koji ima dati prefiks
	for !bytes.HasPrefix(iter.Value().Key, prefix) {
		if !iter.Next() {
			return nil, errors.New("error: could not find prefix")
		}
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
	for !bytes.HasPrefix(iter.Value().Key, iter.prefix) {
		iter.Iterator.current = nil
		return false
	}
	return true
}

// Vraca trenutni zapis iteratora
func (iter *PrefixIterator) Value() *adapter.MemtableEntry {
	return iter.Iterator.Value()
}
