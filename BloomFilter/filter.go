package BloomFilter

import (
	"encoding/binary"
)

type BloomFilter struct {
	HashFunctions []HashWithSeed
	Filter        []bool
}

func MakeBloomFilter(expectedElements int, falsePositive float64) BloomFilter {
	m := CalculateM(expectedElements, falsePositive)
	k := CalculateK(expectedElements, m)
	h := CreateHashFunctions(k)
	return BloomFilter{HashFunctions: h, Filter: make([]bool, m)}
}

func (b *BloomFilter) Add(data []byte) {
	for _, h := range b.HashFunctions {
		b.Filter[h.Hash(data)%uint64(len(b.Filter))] = true
	}
}

func (b *BloomFilter) Read(data []byte) bool {
	for _, h := range b.HashFunctions {
		if !b.Filter[h.Hash(data)%uint64(len(b.Filter))] {
			return false
		}
	}
	return true
}

func (b *BloomFilter) Serialize() []byte {
	serliazied := make([]byte, 0)

	m := make([]byte, 4)
	binary.BigEndian.PutUint32(m, uint32(len(b.Filter)))
	serliazied = append(serliazied, m...)

	k := make([]byte, 4)
	binary.BigEndian.PutUint32(k, uint32(len(b.HashFunctions)))
	serliazied = append(serliazied, k...)

	filter := make([]byte, len(b.Filter))
	for i := 0; i < len(b.Filter); i++ {
		if b.Filter[i] {
			filter[i] = 1
		}
	}
	serliazied = append(serliazied, filter...)

	hashes := make([]byte, 0)
	for i := 0; i < len(b.HashFunctions); i++ {
		seed := b.HashFunctions[i].Seed
		hashes = append(hashes, seed...)
	}
	serliazied = append(serliazied, hashes...)

	return serliazied
}

func Deserialize(data []byte) []BloomFilter {
	bf := make([]BloomFilter, 0)
	for len(data) > 0 {
		m := binary.BigEndian.Uint32(data[:4])
		k := binary.BigEndian.Uint32(data[4:8])

		filter := make([]bool, m)
		for i := 0; i < int(m); i++ {
			if data[8+i] == 1 {
				filter[i] = true
			} else {
				filter[i] = false
			}
		}

		hashes := make([]HashWithSeed, 0)
		for i := 0; i < int(k); i++ {
			seed := data[8+int(m)+i*4 : 8+int(m)+(i+1)*4]
			h := HashWithSeed{Seed: seed}
			hashes = append(hashes, h)
		}

		bf = append(bf, BloomFilter{HashFunctions: hashes, Filter: filter})
		data = data[8+int(m)+int(k)*4:]
	}
	return bf
}
