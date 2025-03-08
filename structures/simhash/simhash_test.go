package simhash

import (
	"fmt"
	"io"
	"os"
	"testing"
)

func TestSimhash(t *testing.T) {
	file1, err := os.Open("./tekst1.txt")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file1.Close()

	file2, err := os.Open("./tekst2.txt")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file2.Close()

	content1, err := io.ReadAll(file1)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	content2, err := io.ReadAll(file2)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	hash1 := SimHash(string(content1))
	hash2 := SimHash(string(content2))

	fmt.Println(CompareHashes(hash1, hash2))
}
