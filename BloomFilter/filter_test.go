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
	serialized := bf.Serialize()
	bf2 := Deserialize(serialized)
	if !bf2[0].Read([]byte("hello")) {
		t.Error("hello should be in the filter")
	}
	if bf2[0].Read([]byte("world")) {
		t.Error("world should not be in the filter")
	}
}
