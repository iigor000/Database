package sstable

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strings"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/structures/block_organization"
	"github.com/iigor000/database/structures/compression"
)

// DataRecord struktura je jedan zapis u Data segmentu SSTable-a
// Tombstone oznacava da li je zapis logicki obrisan
// CRC je kontrolna suma koja se koristi za proveru integriteta podataka
type DataRecord struct {
	Key       []byte
	Value     []byte
	Timestamp int64
	Tombstone bool
	CRC       uint32 // Kontrolna suma za proveru integriteta podataka
	KeySize   int8   // Velicina kljuca
	ValueSize int8   // Velicina vrednosti
	Offset    int    // Offset u fajlu gde je zapis upisan
}

// Data struktura je skup DataRecord-a
type Data struct {
	Records  []DataRecord
	DataFile File
}

// NewDataRecord pravi DataRecord iz memtable entrija
func NewDataRecord(key, value []byte, timestamp int64, tombstone bool) DataRecord {
	record := DataRecord{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
		Tombstone: tombstone,
	}
	// Racunanje CRC pre zapisa u buffer
	record.CRC = record.calcCRC()
	record.KeySize = int8(len(key))
	record.ValueSize = int8(len(value))
	// Postavljanje ofseta na -1, jer jos uvek nije upisan u fajl
	record.Offset = -1

	return record
}

// Serialize serijalizuje DataRecord u bajt niz
func (dr *DataRecord) Serialize(dict *compression.Dictionary) ([]byte, error) {
	var serialized_data []byte
	// Upisujemo CRC
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, dr.CRC)
	serialized_data = append(serialized_data, bytes...)
	// Upisujemo Timestamp
	bytes1 := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes1, uint64(dr.Timestamp))
	serialized_data = append(serialized_data, bytes1...)
	// Upisujemo Tombstone
	if dr.Tombstone {
		serialized_data = append(serialized_data, 1)
	} else {
		serialized_data = append(serialized_data, 0)
	}
	// Upisujemo Key Size ako ne koristimo kompresiju
	if dict == nil {
		serialized_data = append(serialized_data, byte(len(dr.Key)))
		// Upisujemo Value Size ako nije logicki obrisan
		if !dr.Tombstone {
			serialized_data = append(serialized_data, byte(len(dr.Value)))
		}
		// Upisujemo Key
		serialized_data = append(serialized_data, dr.Key...)

		// Upisujemo Value ako nije logicki obrisan
		if !dr.Tombstone {
			serialized_data = append(serialized_data, dr.Value...)

		}
	} else {
		// Upisujemo indeks kljuca u recniku
		index, found := dict.SearchKey(dr.Key)
		if !found {
			return nil, fmt.Errorf("key not found in dictionary")
		}
		bytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(bytes, uint64(index))
		serialized_data = append(serialized_data, bytes...)
		// Upisujemo vrednost samo ako nije logicki obrisana
		if !dr.Tombstone {
			serialized_data = append(serialized_data, byte(len(dr.Value)))
			serialized_data = append(serialized_data, dr.Value...)
		}
	}
	return serialized_data, nil
}

// WriteDataRecord upisuje DataRecord u fajl
func (dr *DataRecord) WriteDataRecord(path string, dict *compression.Dictionary, bm *block_organization.CachedBlockManager) (int, error) {
	serialized_data, err := dr.Serialize(dict)
	if err != nil {
		return -1, fmt.Errorf("error serializing data record: %w", err)
	}
	return bm.Append(path, serialized_data)
}

// calcCRC Racunaa CRC na osnovu Key, Value, Timestamp i Tombstone
func (dr *DataRecord) calcCRC() uint32 {
	// Create a new slice instead of appending to dr.Key
	data := make([]byte, 0, len(dr.Key)+len(dr.Value)+8+1)
	data = append(data, dr.Key...)
	data = append(data, dr.Value...)
	data = append(data, byte(dr.Timestamp>>56), byte(dr.Timestamp>>48), byte(dr.Timestamp>>40), byte(dr.Timestamp>>32),
		byte(dr.Timestamp>>24), byte(dr.Timestamp>>16), byte(dr.Timestamp>>8), byte(dr.Timestamp))
	if dr.Tombstone {
		data = append(data, 1)
	} else {
		data = append(data, 0)
	}
	return crc32.ChecksumIEEE(data)
}

// Upisuje Data u fajl
func (db *Data) WriteData(path string, conf *config.Config, dict *compression.Dictionary, cbm *block_organization.CachedBlockManager) (*Data, error) {

	rec := 0
	for _, record := range db.Records {

		bn, err := record.WriteDataRecord(path, dict, cbm)
		if err != nil {
			return db, fmt.Errorf("error writing data record to file %s: %w", path, err)
		}
		db.Records[rec].Offset = bn * conf.Block.BlockSize // Racunamo ofset kao broj bloka pomnozen sa velicinom bloka

		rec++
		db.DataFile.SizeOnDisk = int64(bn * conf.Block.BlockSize)
	}
	return db, nil
}

// Citanje DataBlock iz fajla
func ReadData(path string, conf *config.Config, dict *compression.Dictionary, startOffset, endOffset int64, bm *block_organization.CachedBlockManager) (*Data, error) {

	block_num := int(startOffset / int64(conf.Block.BlockSize)) // Pocinjemo od bloka koji sadrzi startOffset
	end_block := int(endOffset / int64(conf.Block.BlockSize))   // Kraj bloka koji sadrzi endOffset

	if endOffset <= startOffset {
		end_block = -1 // Kraj bloka koji sadrzi endOffset
	}
	dataBlock := &Data{}

	for {
		block, err := bm.Read(path, block_num)
		if err != nil {
			if err.Error() == "EOF" || strings.Contains(err.Error(), "EOF") {
				break // Kraj fajla
			}
			return nil, fmt.Errorf("error reading data block from file %s: %w", path, err)
		}

		if len(block) == 0 {
			break // Nema vise podataka
		}
		block_num1 := block_num
		for i := 1; i < 1000; i++ {
			if len(block)+(i*1) <= i*conf.Block.BlockSize {
				block_num1 = block_num + i
				break
			}
		}
		if end_block != -1 && block_num1 > end_block {
			break
		}
		record := DataRecord{}
		if err := record.Deserialize(block, dict); err != nil {
			return nil, fmt.Errorf("error deserializing data record: %w", err)
		}
		//dict.Print()
		record.Offset = block_num * conf.Block.BlockSize // Racunamo ofset kao broj bloka pomnozen sa velicinom bloka
		dataBlock.Records = append(dataBlock.Records, record)
		block_num = block_num1

	}
	dataBlock.DataFile = File{
		Path:       path,
		Offset:     startOffset,
		SizeOnDisk: endOffset - startOffset,
	}
	return dataBlock, nil
}

// Deserialize deserializuje DataRecord iz bajt niza
func (dr *DataRecord) Deserialize(data []byte, dict *compression.Dictionary) error {
	//dict.Print()
	if len(data) < 4+8+1 { // CRC + Timestamp + Tombstone
		println("data too short to deserialize DataRecord")
		return fmt.Errorf("data too short to deserialize DataRecord")
	}

	// Citanje CRC
	dr.CRC = binary.LittleEndian.Uint32(data[:4])
	data = data[4:]

	// Citanje Timestamp
	dr.Timestamp = int64(binary.LittleEndian.Uint64(data[:8]))
	data = data[8:]
	// Citanje Tombstone
	dr.Tombstone = data[0] == 1
	data = data[1:]

	if dict == nil {
		if !dr.Tombstone {
			// Citanje Key Size
			if len(data) < 2 {
				println("data too short to read key size")
				return fmt.Errorf("data too short to read key size")
			}
			keySize := int(data[0])
			valueSize := int(data[1])
			data = data[2:]

			if len(data) < keySize {
				println("data too short to read key")
				return fmt.Errorf("data too short to read key")
			}
			dr.Key = data[:keySize]
			data = data[keySize:]
			if len(data) < valueSize {
				println("data too short to read value")
				return fmt.Errorf("data too short to read value")
			}
			dr.Value = data[:valueSize]

		} else {
			dr.Value = nil
			// Citanje Key Size
			if len(data) < 1 {
				println("data too short to read key size")
				return fmt.Errorf("data too short to read key size")
			}
			keySize := int(data[0])
			data = data[1:]
			if len(data) < keySize {
				println("data too short to read key")
				return fmt.Errorf("data too short to read key")
			}
			dr.Key = data[:keySize]

		}
	} else {
		// Ocekivano je da imamo keyindex, valuesize i value
		if len(data) < 8 {
			println("data too short to read key index")
			return fmt.Errorf("data too short to read key index")
		}

		keyIndex := binary.LittleEndian.Uint64(data[:8])
		data = data[8:]
		//println("Key index deserialized:", int(keyIndex))
		var found bool
		dr.Key, found = dict.SearchIndex(int(keyIndex))

		if !found {
			println("key index not found in dictionary:", keyIndex)
			return fmt.Errorf("key index %d not found in dictionary", keyIndex)
		}
		if !dr.Tombstone {
			if len(data) < 1 {
				println("data too short to read value size")
				return fmt.Errorf("data too short to read value size")
			}
			valueSize := int(data[0])
			data = data[1:]
			if len(data) < valueSize {
				println("data too short to read value")
				return fmt.Errorf("data too short to read value")
			}
			dr.Value = data[:valueSize]
			//data = data[valueSize:]
		} else {
			dr.Value = nil // Logicki obrisan zapis nema vrednost
		}
	}

	// Proveravamo CRC
	calculatedCRC := dr.calcCRC()

	if dr.CRC != calculatedCRC {
		println("CRC mismatch")
		println("Expected CRC:", dr.CRC, "Calculated CRC:", calculatedCRC)
		return fmt.Errorf("CRC mismatch: expected %d, got %d", dr.CRC, calculatedCRC)
	}

	return nil
}

// Pomocna funkcija za Iterator-e
func (d *Data) ReadRecord(bm *block_organization.CachedBlockManager, blockNumber int, dict *compression.Dictionary) (adapter.MemtableEntry, int) {

	blockData, err := bm.Read(d.DataFile.Path, blockNumber)
	if err != nil {
		return adapter.MemtableEntry{}, -1
	}

	record := DataRecord{}
	if err := record.Deserialize(blockData, dict); err != nil {
		println("Error deserializing record:", err)
		return adapter.MemtableEntry{}, -1
	}

	record.Offset = blockNumber * bm.BM.BlockSize
	return adapter.MemtableEntry{
		Key:       record.Key,
		Value:     record.Value,
		Timestamp: record.Timestamp,
		Tombstone: record.Tombstone,
	}, blockNumber + 1
}
