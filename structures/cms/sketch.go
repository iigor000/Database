package cms

import "encoding/binary"

// Struktura CountMinSketch
type CountMinSketch struct {
	HashFunctions []HashWithSeed
	Table         [][]uint64
}

// Funckija pravi CountMinSketch preko datih funkcija za hesiranje i parametara epsilon i delta
func MakeCountMinSketch(epsilon float64, delta float64) CountMinSketch {
	m := CalculateM(epsilon)
	k := CalculateK(delta)
	h := CreateHashFunctions(k)

	// Pravimo matricu na ovaj nacin, kako bi niz bio zajedno u memoriji
	matrix := make([][]uint64, k)
	rows := make([]uint64, k*m)
	for i := uint(0); i < k; i++ {
		matrix[i] = rows[i*m : (i+1)*m]
	}

	return CountMinSketch{HashFunctions: h, Table: matrix}
}

// Funckoja za dodavanje kljuca u CountMinSketch, prolazimo sve hash funckije i povecavamo vrednost u tabeli
func (cms CountMinSketch) Add(key string) {
	for i, hashFunction := range cms.HashFunctions {
		hash := hashFunction.Hash([]byte(key)) % uint64(len(cms.Table[0]))
		cms.Table[i][hash]++
	}
}

func (cms CountMinSketch) Read(key string) uint64 {
	// Pravimo maksimalnu vrednost za pocetak (jako velika vrednost, jer smo pomerili binarnu jedinicu za 63 mesta u levo)
	min := uint64(1 << 63)

	// Prolazimo kroz sve hash funkcije i nalazimo minimalnu vrednost
	for i, hashFunction := range cms.HashFunctions {
		hash := hashFunction.Hash([]byte(key)) % uint64(len(cms.Table[0]))
		if cms.Table[i][hash] < min {
			min = cms.Table[i][hash]
		}
	}
	return min
}

func (cms CountMinSketch) Serialize() []byte {
	// Pravimo niz bajtova za serijalizaciju
	serialized := make([]byte, 0)

	// Dodajemo broj kolona
	m := make([]byte, 4)
	binary.BigEndian.PutUint32(m, uint32(len(cms.Table[0])))
	serialized = append(serialized, m...)

	// Dodajemo broj hash funkcija
	k := make([]byte, 4)
	binary.BigEndian.PutUint32(k, uint32(len(cms.HashFunctions)))
	serialized = append(serialized, k...)

	// Dodajemo sve seed-ove hash funkcija
	for _, hashFunction := range cms.HashFunctions {
		serialized = append(serialized, hashFunction.Seed...)
	}

	// Dodajemo sve vrednosti iz tabele
	for _, row := range cms.Table {
		for _, value := range row {
			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, value)
			serialized = append(serialized, v...)
		}
	}

	return serialized
}

func Deserialize(data []byte) []CountMinSketch {
	cms := make([]CountMinSketch, 0)

	for len(data) > 0 {
		m := binary.BigEndian.Uint32(data[:4])
		k := binary.BigEndian.Uint32(data[4:8])

		data = data[8:]

		hashes := make([]HashWithSeed, k)
		for i := 0; i < int(k); i++ {
			hashes[i] = HashWithSeed{Seed: data[i*4 : (i+1)*4]}
		}

		data = data[k*4:]

		matrix := make([][]uint64, k)
		rows := make([]uint64, k*m)
		for i := uint32(0); i < k; i++ {
			matrix[i] = rows[i*m : (i+1)*m]
		}

		for i := uint32(0); i < k; i++ {
			for j := uint32(0); j < m; j++ {
				matrix[i][j] = binary.BigEndian.Uint64(data[(i*m+j)*8 : (i*m+j+1)*8])
			}
		}

		cms = append(cms, CountMinSketch{HashFunctions: hashes, Table: matrix})

		data = data[k*m*8:]
	}

	return cms
}
