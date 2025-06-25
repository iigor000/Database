package sstable

import (
	"fmt"

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
type Index struct {
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

func (ib *Index) WriteIndex(path string, conf *config.Config) error {
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

// ReadIndexBlock cita IndexBlock iz fajla
func ReadIndex(path string, conf *config.Config) (*Index, error) {
	bm := block_organization.NewBlockManager(conf)
	blockNum := 0 // Pocinjemo od prvog bloka
	indexs := &Index{}

	for {
		block, err := bm.ReadBlock(path, blockNum)
		if err != nil {
			if err.Error() == "EOF" {
				break // Kraj fajla
			}
			return nil, err
		}
		if len(block) == 0 {
			break // Nema vise podataka
		}
		record := IndexRecord{}
		if err := record.Deserialize(block); err != nil {
			return nil, err
		}
		record.Offset = blockNum * conf.Block.BlockSize // Racunamo ofset kao broj bloka pomnozen sa velicinom bloka
		indexs.Records = append(indexs.Records, record)
		blockNum++
	}
	return indexs, nil
}

// Deserialize deserializuje IndexRecord iz bajt niza
func (ir *IndexRecord) Deserialize(data []byte) error {
	if len(data) < 1+4 { // Key Size + Offset
		return fmt.Errorf("data too short to read key size and offset")
	}

	// Citanje Key Size
	keySize := int(data[0])
	data = data[1:]

	if len(data) < keySize+4 {
		return fmt.Errorf("data too short to read key and offset")
	}

	// Citanje Key
	ir.Key = data[:keySize]
	data = data[keySize:]

	// Citanje Offset
	ir.Offset = int(data[0])<<24 | int(data[1])<<16 | int(data[2])<<8 | int(data[3])

	return nil
}
