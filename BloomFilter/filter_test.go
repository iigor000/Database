package BloomFilter

import (
	"testing"
)

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
