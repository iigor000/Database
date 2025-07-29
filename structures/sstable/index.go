package sstable

import (
	"bytes"
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
	Key         []byte
	Offset      int
	IndexOffset int
}

// IndexBlock struktura je skup IndexRecord-a
type Index struct {
	Records   []IndexRecord
	IndexFile File
}

// NewIndexRecord pravi IndexRecord
func NewIndexRecord(k []byte, offs int) IndexRecord {
	record := IndexRecord{
		Key:         k,
		Offset:      offs,
		IndexOffset: -1, // Postavljamo IndexOffset na -1 jer jos uvek nije upisan u fajl
	}
	return record
}

func (ib *Index) WriteIndex(path string, conf *config.Config, cbm *block_organization.CachedBlockManager) error {
	rec := 0
	for _, record := range ib.Records {
		i, err := record.WriteIndexRecord(path, cbm)
		if rec == 0 {
			ib.IndexFile.Offset = int64(i) * int64(conf.Block.BlockSize)
		}
		if err != nil {
			return err
		}
		ib.Records[rec].IndexOffset = i * conf.Block.BlockSize // Racunamo IndexOffset kao broj bloka pomnozen sa velicinom bloka
		rec++
		ib.IndexFile.SizeOnDisk = int64(i) * int64(conf.Block.BlockSize)
	}
	ib.IndexFile.Path = path
	ib.IndexFile.SizeOnDisk = ib.IndexFile.SizeOnDisk - ib.IndexFile.Offset
	return nil
}

func (ir *IndexRecord) WriteIndexRecord(path string, bm *block_organization.CachedBlockManager) (int, error) {
	serializedData, _ := ir.Serialize()
	return bm.Append(path, serializedData)
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
func ReadIndex(path string, conf *config.Config, startOffset, endOffset int64, bm *block_organization.CachedBlockManager) (*Index, error) {
	blockNum := int(startOffset / int64(conf.Block.BlockSize)) // Pocinjemo od bloka koji sadrzi startOffset
	endBlock := int(endOffset / int64(conf.Block.BlockSize))   // Kraj bloka koji sadrzi endOffset
	if endOffset <= startOffset {
		endBlock = -1 // Kraj bloka koji sadrzi endOffset
	}
	indexs := &Index{}

	for {
		block, err := bm.Read(path, blockNum)
		if err != nil {
			if err.Error() == "EOF" {
				break // Kraj fajla
			}
			return nil, err
		}
		if len(block) == 0 {
			break // Nema vise podataka
		}
		blockNum1 := blockNum
		i := 1
		for {
			if len(block)+(i*1) <= i*conf.Block.BlockSize {
				blockNum1 += i
				break
			}
			i++
		}
		if endBlock != -1 && blockNum1 > endBlock {
			break // Dostigli smo kraj bloka koji nas zanima
		}
		record := IndexRecord{}
		if err := record.Deserialize(block); err != nil {
			return nil, err
		}
		record.IndexOffset = blockNum * conf.Block.BlockSize // Racunamo ofset kao broj bloka pomnozen sa velicinom bloka
		indexs.Records = append(indexs.Records, record)
		blockNum = blockNum1

	}
	indexs.IndexFile = File{
		Path:       path,
		Offset:     startOffset,
		SizeOnDisk: int64(blockNum)*int64(conf.Block.BlockSize) - startOffset, // Racunamo velicinu fajla kao broj blokova pomnozen sa velicinom bloka minus startOffset
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

// Pomocna funkcija za Iterate
// Index segment nije ucitan iz fajla
func (ib *Index) FindDataOffsetWithKey(indexOffset int, key []byte, bm *block_organization.CachedBlockManager) (int, error) {
	indexRecord := IndexRecord{}
	found := -1
	bnum := indexOffset / bm.BM.BlockSize
	for {
		if ib.IndexFile.SizeOnDisk != -1 {
			if bnum*bm.BM.BlockSize > int(ib.IndexFile.SizeOnDisk+ib.IndexFile.Offset) {
				if found != -1 {
					return found, nil
				}
				return -1, fmt.Errorf("key not found in index")
			}
		}
		serlzdIndexRec, err := bm.Read(ib.IndexFile.Path, bnum)
		if err != nil {
			if err.Error() == "EOF" {
				break // Kraj fajla
			}

			return -1, fmt.Errorf("error reading index block: %w", err)
		}

		if len(serlzdIndexRec) == 0 {
			return -1, fmt.Errorf("key not found in index")
		}
		if err := indexRecord.Deserialize(serlzdIndexRec); err != nil {
			//println("Error deserializing index record:", err)
			return -1, fmt.Errorf("error deserializing index record: %w", err)
		}
		cmp := bytes.Compare(indexRecord.Key, key)
		if cmp == 0 {
			return indexRecord.Offset, nil // Vracamo ofset ako je kljuc pronadjen
		} else if cmp < 0 {
			found = indexRecord.Offset
		} else {
			if found != -1 {
				return found, nil // Vracamo poslednji pronadjeni ofset ako je kljuc manji od trenutnog
			}
			return -1, fmt.Errorf("key not found in index")
		}
		i := 1
		for {
			if len(serlzdIndexRec)+(i*1) <= i*bm.BM.BlockSize {
				bnum += i
				break
			}
			i++
		}
	}
	return -1, fmt.Errorf("key not found in index")
}

func (ib *Index) FindDataOffsetWithPrefix(indexOffset int, key []byte, bm *block_organization.CachedBlockManager) (int, error) {
	indexRecord := IndexRecord{}
	bnum := indexOffset / bm.BM.BlockSize

	for {
		if ib.IndexFile.SizeOnDisk != -1 {
			if bnum*bm.BM.BlockSize > int(ib.IndexFile.SizeOnDisk)+int(ib.IndexFile.Offset) {
				return -1, fmt.Errorf("key not found in index")
			}
		}
		serlzdIndexRec, err := bm.Read(ib.IndexFile.Path, bnum)
		if err != nil {
			if err.Error() == "EOF" {
				break // Kraj fajla
			}
			return -1, fmt.Errorf("error reading index block: %w", err)
		}

		if len(serlzdIndexRec) == 0 {
			return -1, fmt.Errorf("key not found in index")
		}
		if err := indexRecord.Deserialize(serlzdIndexRec); err != nil {
			return -1, fmt.Errorf("error deserializing index record: %w", err)
		}
		if bytes.HasPrefix(indexRecord.Key, []byte(key)) {
			return indexRecord.Offset, nil
		}
		i := 1
		for {
			if len(serlzdIndexRec)+(i*1) <= i*bm.BM.BlockSize {
				bnum += i
				break
			}
			i++
		}
	}
	return -1, fmt.Errorf("key not found in index")
}
