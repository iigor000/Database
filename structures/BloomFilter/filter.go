package BloomFilter

import (
	"encoding/binary"
)

// Struktura za Bloom filter, koristi se niz boolova, jer oni zauzimaju jedan bit u memoriji
type BloomFilter struct {
	HashFunctions []HashWithSeed
	Filter        []bool
}

// Pravimo Bloom filter preko datih funkcija
func MakeBloomFilter(expectedElements int, falsePositive float64) BloomFilter {
	m := CalculateM(expectedElements, falsePositive)
	k := CalculateK(expectedElements, m)
	h := CreateHashFunctions(k)
	return BloomFilter{HashFunctions: h, Filter: make([]bool, m)}
}

// Kada dodajemo u filter, prolazimo kroz hash funckije i za svaku hesiramo element (i racunamo moduo od m) i taj indeks postavljamo na true
func (b *BloomFilter) Add(data []byte) {
	for _, h := range b.HashFunctions {
		b.Filter[h.Hash(data)%uint64(len(b.Filter))] = true
	}
}

// Kada proveravamo da li je element u filteru, prolazimo kroz sve hash funckije i proveravamo da li je indeks dobijen iz njih true
func (b *BloomFilter) Read(data []byte) bool {
	for _, h := range b.HashFunctions {
		if !b.Filter[h.Hash(data)%uint64(len(b.Filter))] {
			return false
		}
	}
	return true
}

// Funkcija za serijalizaciju
func (b *BloomFilter) Serialize() []byte {
	//Pravimo niz bajtova koji ce predstavljati serijalizovani Bloom filter
	serliazied := make([]byte, 0)

	// Upisujemo duzine nizova (k i m) jer ce nam trebati za deserijaizaciju
	m := make([]byte, 4)
	binary.BigEndian.PutUint32(m, uint32(len(b.Filter)))
	serliazied = append(serliazied, m...)

	k := make([]byte, 4)
	binary.BigEndian.PutUint32(k, uint32(len(b.HashFunctions)))
	serliazied = append(serliazied, k...)

	// Filter je niz boolova, pa ga pretvaramo u niz bajtova
	filter := make([]byte, len(b.Filter))
	for i := 0; i < len(b.Filter); i++ {
		if b.Filter[i] {
			filter[i] = 1
		}
	}
	serliazied = append(serliazied, filter...)

	// Serijalizujemo hash funkcije tako sto upisujemo seedove
	hashes := make([]byte, 0)
	for i := 0; i < len(b.HashFunctions); i++ {
		seed := b.HashFunctions[i].Seed
		hashes = append(hashes, seed...)
	}
	serliazied = append(serliazied, hashes...)

	return serliazied
}

// Funckija za deserijalizaciju filtera (moze se ucitati fajl sa vise bloom filtera, pa ova funckija vraca niz filtera)
func Deserialize(data []byte) []BloomFilter {
	// Pravimo niz filtera
	bf := make([]BloomFilter, 0)

	// Proveravamo da li postoji jos filtera u niz bajtova
	for len(data) > 0 {
		// Ucitavamo duzine nizova (k i m)
		m := binary.BigEndian.Uint32(data[:4])
		k := binary.BigEndian.Uint32(data[4:8])

		// Ucitavamo filter (krecemo od 8. bajta jer smo pre toga ucitali duzine)
		filter := make([]bool, m)
		for i := 0; i < int(m); i++ {
			if data[8+i] == 1 {
				filter[i] = true
			} else {
				filter[i] = false
			}
		}

		// Ucitavamo hash funkcije, one su dugacke 4 bajta, pa ih ucitavamo po 4 od indeksa 8+m
		hashes := make([]HashWithSeed, 0)
		for i := 0; i < int(k); i++ {
			seed := data[8+int(m)+i*4 : 8+int(m)+(i+1)*4]
			h := HashWithSeed{Seed: seed}
			hashes = append(hashes, h)
		}

		bf = append(bf, BloomFilter{HashFunctions: hashes, Filter: filter})

		// Skracujemo niz bajtova za duzine koje smo upravo ucitali
		data = data[8+int(m)+int(k)*4:]
	}

	return bf
}
