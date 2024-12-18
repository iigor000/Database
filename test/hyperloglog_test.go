package test

import (
	"testing"

	hll "github.com/iigor000/database/structures/hyperloglog"
)

func TestHLL(t *testing.T) {
	log := hll.MakeHyperLogLog(16)
	log.Add([]byte("bar"))
	log.Add([]byte("foo"))
	log.Add([]byte("baz"))
	log.Add([]byte("qux"))
	log.Add([]byte("quux"))
	log.Add([]byte("corge"))
	log.Add([]byte("grault"))
	log.Add([]byte("garply"))
	log.Add([]byte("waldo"))

	estimation := log.Estimate()
	if estimation < 9 || estimation > 11 {
		t.Errorf("Expected estimation to be around 10, got %f", estimation)
	}
}
