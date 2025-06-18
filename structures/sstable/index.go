package sstable

import (
	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/block_organization"
)

/*
	=== INDEX RECORD ===

	+------------------+---------+---------------+
	|   Key Size (1B)  |   Key   |  Offset (4B)  |
	+------------------+---------+---------------+

*/

// IndexRecord struktura je jedan zapis u Index segmentu SSTable-a
type IndexRecord struct {
	Key    []byte
	Offset int
}

// IndexBlock struktura je skup IndexRecord-a
type IndexBlock struct {
	Records []IndexRecord
}

// NewIndexRecord pravi IndexRecord
func NewIndexRecord(k []byte, offs int) IndexRecord {
	record := IndexRecord{
		Key:    k,
		Offset: offs,
	}
	return record
}

func (ib *IndexBlock) WriteIndex(path string, conf *config.Config) error {
	bm := block_organization.NewBlockManager(conf)
	for _, record := range ib.Records {
		err := record.WriteIndexRecord(path, bm)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ir *IndexRecord) WriteIndexRecord(path string, bm *block_organization.BlockManager) error {
	serializedData, _ := ir.Serialize()
	_, err := bm.AppendBlock(path, serializedData)
	if err != nil {
		return err
	}
	return nil
}

func (ir *IndexRecord) Serialize() ([]byte, int) {
	var serializedData []byte
	rec_size := 1 + len(ir.Key) + 4 // 1 byte for key length, len(ir.Key) bytes for key, 4 bytes for offset
	serializedData = append(serializedData, byte(len(ir.Key)))
	serializedData = append(serializedData, ir.Key...)
	serializedData = append(serializedData, byte(ir.Offset>>24), byte(ir.Offset>>16), byte(ir.Offset>>8), byte(ir.Offset))
	return serializedData, rec_size
}
