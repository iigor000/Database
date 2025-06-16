package sstable

import (
	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/block_organization"
)

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
	block_size := conf.Block.BlockSize
	block_num := 0
	temp_block_size := 0
	for _, record := range ib.Records {
		err, rec_size := record.WriteIndexRecord(path, bm, block_num)
		if err != nil {
			return err
		}
		temp_block_size += rec_size
		if temp_block_size >= block_size {
			block_num++
			temp_block_size = 0
		}
	}

	return nil
}

func (ir *IndexRecord) WriteIndexRecord(path string, bm *block_organization.BlockManager, i int) (error, int) {
	serializedData, rec_size := ir.Serialize()
	err := bm.AppendBlock(path, i, serializedData)
	if err != nil {
		return err, 0
	}
	return nil, rec_size
}

func (ir *IndexRecord) Serialize() ([]byte, int) {
	var serializedData []byte
	rec_size := 1 + len(ir.Key) + 4 // 1 byte for key length, len(ir.Key) bytes for key, 4 bytes for offset
	serializedData = append(serializedData, byte(len(ir.Key)))
	serializedData = append(serializedData, ir.Key...)
	serializedData = append(serializedData, byte(ir.Offset>>24), byte(ir.Offset>>16), byte(ir.Offset>>8), byte(ir.Offset))
	return serializedData, rec_size
}
