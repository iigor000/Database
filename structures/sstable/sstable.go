package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"time"

	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/structures/block_organization"
	"github.com/iigor000/database/structures/bloomfilter"
	"github.com/iigor000/database/structures/memtable"
	"github.com/iigor000/database/structures/merkle"
)

// TODO CONFIG
const SummaryDegree = 5 // Stepen proređenosti Summary strukture (nonzero integer)

type SSTable struct {
	DataFilePath  string
	IndexFilePath string
	Filter        bloomfilter.BloomFilter
	Summary       *Summary
	Metadata      *merkle.MerkleTree
}

type Index struct {
	Key    int
	Offset int // Offset za citanje u index strukturi u kojoj se nalazi Offset za citanje u Data strukturi
}

type Summary struct {
	MinKey  int
	MaxKey  int
	Indexes []Index
}

func (sst *SSTable) GetSize() int64 {
	dataInfo, err := os.Stat(sst.DataFilePath)
	if err != nil {
		return 0
	}
	indexInfo, err := os.Stat(sst.IndexFilePath)
	if err != nil {
		return 0
	}

	// Vraćanje ukupne veličine
	return dataInfo.Size() + indexInfo.Size()
}

func (sst *SSTable) GetDataPath() string {
	return sst.DataFilePath
}

func (sst *SSTable) GetIndexPath() string {
	return sst.IndexFilePath
}

func calculateCRC(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func prepareIndexData(offset int, key int) ([]byte, error) {
	var buf bytes.Buffer

	// 1. Key (8B)
	if err := binary.Write(&buf, binary.BigEndian, uint64(key)); err != nil {
		return nil, err
	}

	// 2. Offset (8B)
	if err := binary.Write(&buf, binary.BigEndian, uint64(offset)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func prepareData(entry adapter.MemtableEntry) ([]byte, error) {
	var buf bytes.Buffer

	// Varijabilni Enkoding Ključa
	keyBuf := make([]byte, binary.MaxVarintLen64)
	keyLen := binary.PutUvarint(keyBuf, uint64(entry.Key))
	keyBytes := keyBuf[:keyLen]

	// Računamo CRC za Timestamp + Tombstone + Key Size + Val Size + Key + Value
	crcBuf := bytes.Buffer{}

	// Timestamp (16B)
	if err := binary.Write(&crcBuf, binary.BigEndian, entry.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Write(&crcBuf, binary.BigEndian, int64(0)); err != nil { // dopuna do 16B
		return nil, err
	}

	// Tombstone (1B)
	var tombstoneByte byte
	if entry.Tombstone {
		tombstoneByte = 0x01
	}
	if err := crcBuf.WriteByte(tombstoneByte); err != nil {
		return nil, err
	}

	// Key Size (8B)
	if err := binary.Write(&crcBuf, binary.BigEndian, uint64(len(keyBytes))); err != nil {
		return nil, err
	}

	if !entry.Tombstone {
		// Val Size (8B)
		if err := binary.Write(&crcBuf, binary.BigEndian, uint64(len(entry.Value))); err != nil {
			return nil, err
		}
	}

	// 6. Key (Key size B)
	if _, err := crcBuf.Write(keyBytes); err != nil {
		return nil, err
	}

	if !entry.Tombstone {
		// 7. Value (Value size B)
		if _, err := buf.Write(entry.Value); err != nil {
			return nil, err
		}
	}

	// Prvo upisujemo CRC (4B)
	crc := calculateCRC(crcBuf.Bytes())
	if err := binary.Write(&buf, binary.BigEndian, crc); err != nil {
		return nil, err
	}

	// Ostatak kopiramo u buffer
	if _, err := buf.Write(crcBuf.Bytes()); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Kreira SSTable strukturu iz Memtable-a (poziva se pri Flush-ovanju)
func NewSSTable(dataFilePath string, indexFilePath string, memtable *memtable.Memtable, blockManager *block_organization.BlockManager) *SSTable {
	filter := bloomfilter.MakeBloomFilter(memtable.size, 0.01) // TODO da li ovih 1% treba u config, ne piše u specifikaciji

	summ := &Summary{}

	// ubaciti i sve podatke u BloomFilter i u SSTable (u Data, Index i Summary strukture)
	dataOffset := 0
	dataBlockNumber := 0
	writeData := make([]byte, 0, blockManager.BlockSize)
	indexOffset := 0
	indexBlockNumber := 0
	writeIndexData := make([]byte, 0, blockManager.BlockSize)
	firstKey := -1
	lastKey := -1
	count := 0
	for data := range memtable.Data {
		if firstKey == -1 {
			firstKey = data.Key
		}
		lastKey = data.Key

		// Dodajem key u BloomFilter
		filter.Add(data.Key)

		// Pripremam podatke za upis u SSTable
		bytesData, err := prepareData(data)
		if err != nil {
			panic(fmt.Sprintf("Failed to prepare data: %v", err))
		}

		dataLength := len(bytesData)

		indexFileOffset := dataBlockNumber*blockManager.BlockSize + dataOffset
		indexData, err := prepareIndexData(indexFileOffset, data.Key)
		if err != nil {
			panic(fmt.Sprintf("Failed to prepare data: %v", err))
		}

		indexLength := len(indexData)

		// UPIS U INDEX DATOTEKU

		if indexOffset+indexLength > blockManager.BlockSize {
			// Upisujemo prethodni blok u datoteku
			err = blockManager.WriteBlock(indexFilePath, indexBlockNumber, writeIndexData)
			if err != nil {
				panic(fmt.Sprintf("WriteBlock failed: %v", err))
			}
			indexBlockNumber++
			indexOffset = 0
			// Kreiramo novi blok
			writeIndexData = make([]byte, 0, blockManager.BlockSize)
			// Dodajemo novi podatak u novi blok
		}

		// Upisujem Key i Offset u Summary strukturu
		i := Index{Key: int(data.Key), Offset: int(indexBlockNumber*blockManager.BlockSize + indexOffset)}
		if count == 0 {
			summ.Indexes = append(summ.Indexes, i)
		}
		count = (count + 1) % SummaryDegree

		writeIndexData = append(writeIndexData, indexData...)
		indexOffset += indexLength

		// UPIS U DATA DATOTEKU

		// Proveravam da li je potrebno da upišem prethodni blok u datoteku
		if dataOffset+dataLength > blockManager.BlockSize {
			// Upisujemo prethodni blok u datoteku
			err = blockManager.WriteBlock(dataFilePath, dataBlockNumber, writeData)
			if err != nil {
				panic(fmt.Sprintf("WriteBlock failed: %v", err))
			}
			dataBlockNumber++
			dataOffset = 0
			// Kreiramo novi blok
			writeData = make([]byte, 0, blockManager.BlockSize)
			// Dodajemo novi podatak u novi blok
		}
		writeData = append(writeData, bytesData...)
		dataOffset += dataLength
	}

	summ.MinKey = firstKey
	summ.MaxKey = lastKey
	if SummaryDegree > 1 && count != 1 {
		// Ako je SummaryDegree > 1 (ne dodajemo svaki element)
		// i ako poslednji element nije upisan u Summary strukturu
		// dodajemo ga na kraju
		i := Index{Key: int(lastKey), Offset: int(indexBlockNumber*blockManager.BlockSize + indexOffset)}
		summ.Indexes = append(summ.Indexes, i)
	}

	err := blockManager.WriteBlock(dataFilePath, dataBlockNumber, writeData)
	if err != nil {
		panic(fmt.Sprintf("WriteBlock failed: %v", err))
	}

	err = blockManager.WriteBlock(indexFilePath, indexBlockNumber, writeIndexData)
	if err != nil {
		panic(fmt.Sprintf("WriteBlock failed: %v", err))
	}

	merkleTree := merkle.NewMerkleTree(memtable.Data)

	// Kreiraj SSTable strukturu
	return &SSTable{
		DataFilePath:  dataFilePath,
		IndexFilePath: indexFilePath,
		Filter:        filter,
		Summary:       summ,
		Metadata:      merkleTree,
	}
}

// Kreira SSTable strukturu iz više SSTable-a (prilikom level-ovanja)
func CreateSSTable(entries []adapter.MemtableEntry, dataFilePath string, indexFilePath string, bm *block_organization.BlockManager) *SSTable {
	filter := bloomfilter.MakeBloomFilter(len(entries), 0.01) // TODO Isto kao kod flush-a

	summ := &Summary{}

	dataOffset := 0
	dataBlockNumber := 0
	writeData := make([]byte, 0, bm.BlockSize)

	indexOffset := 0
	indexBlockNumber := 0
	writeIndexData := make([]byte, 0, bm.BlockSize)

	firstKey := -1
	lastKey := -1
	count := 0

	for _, entry := range entries {
		if firstKey == -1 {
			firstKey = entry.Key
		}
		lastKey = entry.Key

		// TODO
		filter.Add(entry.Key)

		bytesData, err := prepareData(entry)
		if err != nil {
			panic(fmt.Sprintf("Failed to prepare data: %v", err))
		}
		dataLength := len(bytesData)

		indexFileOffset := dataBlockNumber*bm.BlockSize + dataOffset
		indexData, err := prepareIndexData(indexFileOffset, entry.Key)
		if err != nil {
			panic(fmt.Sprintf("Failed to prepare index: %v", err))
		}
		indexLength := len(indexData)

		// Upisujemo blok u index fajl
		if indexOffset+indexLength > bm.BlockSize {
			err = bm.WriteBlock(indexFilePath, indexBlockNumber, writeIndexData)
			if err != nil {
				panic(fmt.Sprintf("Failed to write index block: %v", err))
			}
			indexBlockNumber++
			indexOffset = 0
			writeIndexData = make([]byte, 0, bm.BlockSize)
		}

		i := Index{Key: entry.Key, Offset: indexBlockNumber*bm.BlockSize + indexOffset}
		if count == 0 {
			summ.Indexes = append(summ.Indexes, i)
		}
		count = (count + 1) % SummaryDegree

		writeIndexData = append(writeIndexData, indexData...)
		indexOffset += indexLength

		// Upisujemo blok u data fajl
		if dataOffset+dataLength > bm.BlockSize {
			err = bm.WriteBlock(dataFilePath, dataBlockNumber, writeData)
			if err != nil {
				panic(fmt.Sprintf("Failed to write data block: %v", err))
			}
			dataBlockNumber++
			dataOffset = 0
			writeData = make([]byte, 0, bm.BlockSize)
		}

		writeData = append(writeData, bytesData...)
		dataOffset += dataLength
	}

	// Upis poslednjih blokova
	if len(writeData) > 0 {
		err := bm.WriteBlock(dataFilePath, dataBlockNumber, writeData)
		if err != nil {
			panic(fmt.Sprintf("Failed to write final data block: %v", err))
		}
	}

	if len(writeIndexData) > 0 {
		err := bm.WriteBlock(indexFilePath, indexBlockNumber, writeIndexData)
		if err != nil {
			panic(fmt.Sprintf("Failed to write final index block: %v", err))
		}
	}

	// Ako treba poslednji Summary element
	if SummaryDegree > 1 && count != 1 {
		i := Index{Key: lastKey, Offset: indexBlockNumber*bm.BlockSize + indexOffset}
		summ.Indexes = append(summ.Indexes, i)
	}

	// TODO
	merkleTree := merkle.NewMerkleTree(entries)

	return &SSTable{
		DataFilePath:  dataFilePath,
		IndexFilePath: indexFilePath,
		Filter:        filter,
		Summary:       summ,
		Metadata:      merkleTree,
	}
}

func (sst *SSTable) GetIndexAt(checkKey int, offset int, blockManager *block_organization.BlockManager) int {
	indexOffset := offset / blockManager.BlockSize

	indexData, err := blockManager.ReadBlock(sst.IndexFilePath, indexOffset)
	if err != nil {
		panic(fmt.Sprintf("Failed to read index file: %v", err))
	}

	idx := offset % blockManager.BlockSize

	idxKey := binary.BigEndian.Uint64(indexData[idx : idx+8])
	idxOffset := binary.BigEndian.Uint64(indexData[idx+8 : idx+16])

	if int(idxKey) == checkKey {
		return int(idxOffset)
	} else {
		return -1
	}
}

func (sst *SSTable) GetDataAt(offset int, blockManager *block_organization.BlockManager) []byte {
	if offset < 0 {
		return nil
	}

	dataOffset := offset / blockManager.BlockSize

	data, err := blockManager.ReadBlock(sst.DataFilePath, dataOffset)
	if err != nil {
		panic(fmt.Sprintf("Failed to read data file: %v", err))
	}

	dataIdx := offset % blockManager.BlockSize

	crc := data[dataIdx : dataIdx+4]
	dataIdx += 20 // skip timestamp
	tombstone := data[dataIdx : dataIdx+1]
	dataIdx += 1
	keySize := data[dataIdx : dataIdx+8]
	dataIdx += 8

	if tombstone[0] == 0x01 {
		return nil
	} else {
		valueSize := data[dataIdx : dataIdx+8]
		dataIdx += 8

		if len(data) < dataIdx+int(keySize[0])+int(valueSize[0]) {
			return nil
		}
		key := data[dataIdx : dataIdx+int(keySize[0])]

		dataIdx += int(keySize[0])
		value := data[dataIdx : dataIdx+int(valueSize[0])]

		if calculateCRC(append(key, value...)) != binary.BigEndian.Uint32(crc) {
			return nil
		}

		return value
	}
}

func (sst *SSTable) DeleteDataAt(offset int, blockManager *block_organization.BlockManager) {
	if offset < 0 {
		return
	}

	dataOffset := offset / blockManager.BlockSize

	data, err := blockManager.ReadBlock(sst.DataFilePath, dataOffset)
	if err != nil {
		panic(fmt.Sprintf("Failed to read data file: %v", err))
	}

	dataIdx := offset % blockManager.BlockSize

	off := 0
	crc := data[dataIdx+off : dataIdx+off+4]
	off += 20 // skip timestamp
	tombstone := data[dataIdx+off : dataIdx+off+1]

	if tombstone[0] == 0x01 {
		return
	}

	off += 1
	keySize := binary.BigEndian.Uint64(data[dataIdx+off : dataIdx+off+8])
	off += 8

	valueSize := binary.BigEndian.Uint64(data[dataIdx+off : dataIdx+off+8])
	off += 8

	if len(data) < dataIdx+int(keySize)+int(valueSize) {
		return
	}
	key := data[dataIdx+off : dataIdx+off+int(keySize)]

	off += int(keySize)
	value := data[dataIdx+off : dataIdx+off+int(valueSize)]
	off += int(valueSize)

	if calculateCRC(append(key, value...)) != binary.BigEndian.Uint32(crc) {
		return
	}

	// Upisati novi podatak u SSTable sa Tombstone = true
	entry := adapter.MemtableEntry{
		Key:       int(binary.BigEndian.Uint32(key)),
		Value:     nil, // nebitno
		Timestamp: time.Now().UnixNano(),
		Tombstone: true,
	}

	bytesData, err := prepareData(entry)
	if err != nil {
		panic(fmt.Sprintf("Failed to prepare data: %v", err))
	}

	dataLength := len(bytesData)

	dataIdx = offset % blockManager.BlockSize

	if len(data)-dataIdx < len(bytesData) {
		return
	}

	// Kreiramo novi blok bez val size i value
	copy(data[dataIdx:], bytesData)
	copy(data[dataIdx+dataLength:], data[dataIdx+off:])

	err = blockManager.WriteBlock(sst.DataFilePath, dataOffset, data)
	if err != nil {
		panic(fmt.Sprintf("WriteBlock failed: %v", err))
	}

	// TODO ovde treba ukloniti podatak iz BloomFilter-a
	//sst.Filter.Delete(int(key[0]))
	// isto i za merkle tree
}

func (sst *SSTable) GetIndexBetween(key, offsetMin, offsetMax int, blockManager *block_organization.BlockManager) (int, bool) {
	offsetBlock := offsetMin / blockManager.BlockSize

	indexData, err := blockManager.ReadBlock(sst.IndexFilePath, offsetBlock)
	if err != nil {
		panic(fmt.Sprintf("Failed to read index file: %v", err))
	}

	indexOffsetMin := offsetMin % blockManager.BlockSize
	indexOffsetMax := offsetMax % blockManager.BlockSize

	for i := indexOffsetMin; i < indexOffsetMax; i += 16 {
		if i+16 > len(indexData) {
			break
		}

		foundKey := binary.BigEndian.Uint64(indexData[i : i+8])
		offset := binary.BigEndian.Uint64(indexData[i+8 : i+16])

		if foundKey == uint64(key) {
			return int(offset), true
		}
	}

	return -1, false
}

func (sst *SSTable) Search(key int, blockManager *block_organization.BlockManager) ([]byte, bool) {
	// TODO Proveri da li je ključ u Bloom filteru
	if !sst.Filter.Read(key) {
		return nil, false
	}

	// Ako jeste, proveri da li je ključ možda u Summary strukturi
	if key < sst.Summary.MinKey || key > sst.Summary.MaxKey {
		return nil, false
	}

	// Pronađi ključ ili pronađi između kojih ključeva se on možda nalazi u Index Fajlu
	offsetMin := sst.Summary.Indexes[0].Offset
	offsetMax := sst.Summary.Indexes[len(sst.Summary.Indexes)-1].Offset
	for _, entry := range sst.Summary.Indexes {
		if entry.Key == key {
			return sst.GetDataAt(sst.GetIndexAt(key, entry.Offset, blockManager), blockManager), true
		} else if SummaryDegree > 1 {
			if entry.Key > key {
				offsetMax = entry.Offset
				break
			}
			offsetMin = entry.Offset
			continue
		}
	}

	index, found := sst.GetIndexBetween(key, offsetMin, offsetMax, blockManager)

	if !found {
		return nil, false
	}

	return sst.GetDataAt(index, blockManager), true
}

func (sst *SSTable) Delete(key int, blockManager *block_organization.BlockManager) {
	// TODO Proveri da li je ključ u Bloom filteru
	if !sst.Filter.Read(key) {
		return
	}

	// Ako jeste, proveri da li je ključ možda u Summary strukturi
	if key < sst.Summary.MinKey || key > sst.Summary.MaxKey {
		return
	}

	// Pronađi ključ ili pronađi između kojih ključeva se on možda nalazi u Index Fajlu
	offsetMin := sst.Summary.Indexes[0].Offset
	offsetMax := sst.Summary.Indexes[len(sst.Summary.Indexes)-1].Offset
	for _, entry := range sst.Summary.Indexes {
		if entry.Key == key {
			sst.DeleteDataAt(sst.GetIndexAt(key, entry.Offset, blockManager), blockManager)
		} else if SummaryDegree > 1 {
			if entry.Key > key {
				offsetMax = entry.Offset
				break
			}
			offsetMin = entry.Offset
			continue
		}
	}

	index, _ := sst.GetIndexBetween(key, offsetMin, offsetMax, blockManager)

	sst.DeleteDataAt(index, blockManager)
}

func (sst *SSTable) ReadAll(blockManager *block_organization.BlockManager) []adapter.MemtableEntry {
	entries := make([]adapter.MemtableEntry, 0)

	dataBlockNumber := 0

	for {
		block, err := blockManager.ReadBlock(sst.DataFilePath, dataBlockNumber)
		if err != nil {
			break
		}

		idx := 0
		for idx < len(block) {
			// Minimalni blok bez key, value para: 4(CRC) + 16(TS) + 1(TS) + 8(KS) = 29 bytes
			if idx+29 > len(block) {
				break
			}

			crc := binary.BigEndian.Uint32(block[idx : idx+4])
			idx += 4

			timestampBytes := block[idx : idx+16]
			timestamp := int64(binary.BigEndian.Uint64(timestampBytes[8:]))
			idx += 16

			tombstone := block[idx] == 0x01
			idx += 1

			keySize := binary.BigEndian.Uint64(block[idx : idx+8])
			idx += 8

			if tombstone {
				// Preskačemo tombstone polja
				idx += int(keySize)
				continue
			}

			valueSize := binary.BigEndian.Uint64(block[idx : idx+8])
			idx += 8

			if idx+int(keySize)+int(valueSize) > len(block) {
				break
			}

			keyBytes := block[idx : idx+int(keySize)]
			key := int(binary.BigEndian.Uint32(keyBytes))
			idx += int(keySize)

			value := block[idx : idx+int(valueSize)]
			idx += int(valueSize)

			// check sum
			checksum := calculateCRC(append(keyBytes, value...))
			if checksum != crc {
				continue
			}

			// Ako je sve okej, dodajemo u listu
			entry := adapter.MemtableEntry{
				Key:       key,
				Value:     value,
				Timestamp: timestamp,
				Tombstone: tombstone,
			}
			entries = append(entries, entry)
		}

		dataBlockNumber++
	}

	return entries
}

func (sst *SSTable) ValidateMerkleTree() bool {
	// TODO Provera integriteta podataka
	return true
}
