package hyperloglog

import (
	"testing"
)

func TestHLL(t *testing.T) {
	log, err := MakeHyperLogLog(16)
	if err != nil {
		t.Fatalf("Failed to create HyperLogLog: %v", err)
	}
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
