package compression

import (
	"encoding/binary"
	"os"
)

// Dictionary predstavlja strukturu koja mapira kljuceve na indekse
// svaki kljuc tacnije string je predstavljen celim brojem
type Dictionary struct {
	keys     [][]byte
	indexMap map[string]int
}

// NewDictionary kreira novi Dictionary
func NewDictionary() *Dictionary {
	return &Dictionary{
		keys:     make([][]byte, 0),
		indexMap: make(map[string]int),
	}
}

// Add dodaje kljuc u Dictionary i vraca njegov indeks
// ako kljuc vec postoji, vraca njegov indeks
func (d *Dictionary) Add(key []byte) int {
	index, exists := d.indexMap[string(key)]
	if exists {
		return index
	}
	d.keys = append(d.keys, key)
	index = len(d.keys) - 1
	d.indexMap[string(key)] = index
	return index
}

func (d *Dictionary) SearchIndex(index int) ([]byte, bool) {
	if index < 0 || index >= len(d.keys) {
		return nil, false
	}
	return d.keys[index], true
}

func (d *Dictionary) SearchKey(key []byte) (int, bool) {
	index, exists := d.indexMap[string(key)]
	if !exists {
		return -1, false
	}
	return index, true
}

// Encode pretvara Dictionary u niz bajtova
func (d *Dictionary) Serialize() []byte {
	encoded := make([]byte, 0)
	for _, key := range d.keys {
		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, uint64(len(key)))
		encoded = append(encoded, buf[:n]...)
		encoded = append(encoded, key...)
	}
	return encoded
}

// Decode pretvara niz bajtova nazad u Dictionary
func Deserialize(data []byte) (*Dictionary, bool) {
	d := NewDictionary()
	i := 0
	for i < len(data) {
		keylen, n := binary.Uvarint(data[i:])
		if n <= 0 {
			return nil, false
		}
		i = i + n
		if i+int(keylen) > len(data) {
			return nil, false
		}
		key := data[i : i+int(keylen)]
		d.Add(key)
		i = i + int(keylen)
	}
	return d, true
}

// Citanje iz fajla
func Read(path string) (*Dictionary, error) {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return NewDictionary(), nil // Fajl ne postoji, vracamo prazan Dictionary
	}
	if err != nil {
		return nil, err // Drugi greska pri proveri fajla
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err // Greska pri citanju fajla
	}
	dict, pass := Deserialize(data)
	if !pass {
		return nil, err // Greska pri dekodiranju
	}
	return dict, nil
}

// Pisanje u fajl
func (d *Dictionary) Write(path string) error {
	f, err := os.Create(path)
	if os.IsNotExist(err) {
		return os.ErrNotExist // Fajl ne postoji
	} else if err != nil {
		return err // Druga greska pri otvaranju fajla
	}
	defer f.Close()
	encoded := d.Serialize()
	_, err = f.Write(encoded)
	if err != nil {
		return err // Greska pri pisanju u fajl
	}
	return nil
}

func (d *Dictionary) IsEmpty() bool {
	return len(d.keys) == 0
}

func (d *Dictionary) Print() {
	for i, key := range d.keys {
		println("Index:", i, "Key:", string(key))
	}
}
