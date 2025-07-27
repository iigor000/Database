package skiplist

import (
	"bytes"
	"errors"

	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/util"
)

type Iterator struct {
	current *Node
	value   adapter.MemtableEntry
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
	if current.next == nil {
		return nil, errors.New("no entries found")
	}
	current = current.next

	return &Iterator{current: current, value: deserializeEntry(current.value)}, nil
}

// Prebacujemo iterator na sledeci cvor, ako nije prazan, ili ako je kljuc rezervisan idemo na sledeci
func (iter *Iterator) Next() (adapter.MemtableEntry, bool) {
	if iter.current == nil {
		return adapter.MemtableEntry{}, false // Nema vise zapisa
	}

	oldValue := iter.value

	for iter.current.next != nil {
		iter.current = iter.current.next
		if !util.CheckKeyReserved(string(iter.current.key)) {
			iter.value = deserializeEntry(iter.current.value)
			return oldValue, true
		}
	}

	iter.Stop()
	return oldValue, true
}

// Vraca trenutni zapis iteratora
func (iter *Iterator) Stop() {
	iter.current = nil
	iter.value = adapter.MemtableEntry{}
}

// Inicijalizuje iterator koji vraca samo zapise sa datim opsegom kljuceva
func (s *SkipList) NewRangeIterator(startKey []byte, endKey []byte) (*RangeIterator, error) {
	if s.isEmpty() {
		return nil, errors.New("skiplist is empty")
	}

	current := s.root
	for current.down != nil {
		current = current.down
	}
	if current.next == nil {
		return nil, errors.New("no entries found with the given range")
	}
	current = current.next

	for current != nil && bytes.Compare(current.key, startKey) < 0 {
		current = current.next
	}

	iter := &Iterator{current: current, value: deserializeEntry(current.value)}

	return &RangeIterator{
		Iterator: *iter,
		startKey: startKey,
		endKey:   endKey,
	}, nil
}

// Prolazi kroz iterator i vraca samo one zapise koji su u opsegu startKey i endKey
func (iter *RangeIterator) Next() (adapter.MemtableEntry, bool) {
	if iter.Iterator.current == nil {
		return adapter.MemtableEntry{}, false // Nema vise zapisa
	}

	oldValue := iter.value

	value, ok := iter.Iterator.Next()
	if !ok {
		return adapter.MemtableEntry{}, false
	}

	if bytes.Compare(value.Key, iter.endKey) > 0 {
		iter.Iterator.current = nil
		iter.Stop()
		return oldValue, true
	}

	return oldValue, true
}

// Inicijalizuje iterator koji vraca samo zapise sa datim prefiksom
func (s *SkipList) NewPrefixIterator(prefix []byte) (*PrefixIterator, error) {
	if s.isEmpty() {
		return nil, errors.New("skiplist is empty")
	}

	current := s.root
	for current.down != nil {
		current = current.down
	}

	if current.next == nil {
		return nil, errors.New("no entries found with the given prefix")
	}
	current = current.next

	// Postavljamo iterator na prvi kljuc koji ima dati prefiks
	for !bytes.HasPrefix(current.key, prefix) {
		if current.next == nil {
			return nil, errors.New("error: could not find prefix")
		}
		current = current.next
	}

	iter := &Iterator{current: current, value: deserializeEntry(current.value)}

	return &PrefixIterator{
		Iterator: *iter,
		prefix:   prefix,
	}, nil
}

// Prolazi kroz iterator i vraca samo one zapise koji imaju dati prefiks
func (iter *PrefixIterator) Next() (adapter.MemtableEntry, bool) {
	if iter.Iterator.current == nil {
		return adapter.MemtableEntry{}, false // Nema vise zapisa
	}
	oldValue := iter.value

	value, ok := iter.Iterator.Next()
	if !ok {
		return adapter.MemtableEntry{}, false
	}

	for !bytes.HasPrefix(value.Key, iter.prefix) {
		iter.Stop()
		return oldValue, true
	}
	return oldValue, true
}
