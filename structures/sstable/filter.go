package sstable

import (
	"os"

	"github.com/iigor000/database/structures/bloomfilter"
)

type FilterBlock struct {
	Filter bloomfilter.BloomFilter
}

// NewFilterBlock creates a new FilterBlock with the given BloomFilter
func NewFilterBlock(filter bloomfilter.BloomFilter) *FilterBlock {
	return &FilterBlock{
		Filter: filter,
	}
}

// NewFilterBlockFromBytes deserializes a byte slice into a FilterBlock
func NewFilterBlockFromBytes(data []byte) (*FilterBlock, error) {
	filter := bloomfilter.Deserialize(data)
	return &FilterBlock{
		Filter: filter[0],
	}, nil
}

// Serialize serializes the FilterBlock to a file
func (fb *FilterBlock) Serialize(path string, generation int) error {
	buf := fb.FilterToBytes()
	filename := CreateFileName(path, generation, "Filter", "db")
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(buf)
	return err
}

// DeserializeFilterBlock deserializes a FilterBlock from a file
func DeserializeFilterBlock(path string, generation int) (*FilterBlock, error) {
	filename := CreateFileName(path, generation, "Filter", "db")
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	filterBlock, err := NewFilterBlockFromBytes(data)
	if err != nil {
		return nil, err
	}
	return filterBlock, nil
}
