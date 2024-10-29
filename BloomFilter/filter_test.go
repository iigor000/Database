package BloomFilter

import (
	"testing"
)

// Test proverava da li radi upisivanje i citanje iz Bloom filtera
func TestBloomFilter(t *testing.T) {
	bf := MakeBloomFilter(1000, 0.01)
	bf.Add([]byte("hello"))
	bf.Add([]byte("nesto"))
	if !bf.Read([]byte("hello")) {
		t.Error("hello should be in the filter")
	}
	if bf.Read([]byte("world")) {
		t.Error("world should not be in the filter")
	}
}

// Test proverava da li radi serijalizacija i deserijalizacija Bloom filtera
func TestSerializaton(t *testing.T) {
	bf := MakeBloomFilter(1000, 0.01)
	bf.Add([]byte("hello"))
	bf.Add([]byte("nesto"))

	bf2 := MakeBloomFilter(1000, 0.01)
	bf2.Add([]byte("world"))

	serialized1 := bf.Serialize()
	serialized2 := bf2.Serialize()
	deserialized := Deserialize(append(serialized1, serialized2...))

	bf3 := deserialized[0]
	bf4 := deserialized[1]

	if !bf3.Read([]byte("hello")) {
		t.Error("hello should be in the filter")
	}
	if bf3.Read([]byte("world")) {
		t.Error("world should not be in the filter")
	}
	if !bf4.Read([]byte("world")) {
		t.Error("world should be in the filter")
	}
	if bf4.Read([]byte("hello")) {
		t.Error("hello should not be in the filter")
	}
}
