package sstable

import (
	"fmt"
	"io"
	"os"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/structures/block_organization"
	"github.com/iigor000/database/structures/bloomfilter"
	"github.com/iigor000/database/structures/compression"
	"github.com/iigor000/database/structures/memtable"
	"github.com/iigor000/database/structures/merkle"
)

// SSTable struktura
type SSTable struct {
	Data           *Data
	Index          *Index
	Summary        *Summary
	Filter         bloomfilter.BloomFilter
	Metadata       *merkle.MerkleTree
	Gen            int
	UseCompression bool
	CompressionKey *compression.Dictionary
	Dir            string
	SingleFile     bool // Da li se SSTable cuva u jednom fajlu ili u vise
}

// NewSSTable kreira novi SSTable
func FlushSSTable(conf *config.Config, memtable memtable.Memtable, generation int) *SSTable {
	var sstable SSTable
	sstable.UseCompression = conf.SSTable.UseCompression
	sstable.Gen = generation
	path := fmt.Sprintf("%s/%d", conf.SSTable.SstableDirectory, generation)
	err := CreateDirectoryIfNotExists(path)
	if err != nil {
		panic("Error creating directory for SSTable: " + err.Error())
	}
	sstable.SingleFile = conf.SSTable.SingleFile
	sstable.Data, sstable.CompressionKey = buildData(memtable, conf, generation, path, sstable.SingleFile)
	sstable.Index = buildIndex(conf, generation, path, sstable.Data, sstable.SingleFile)
	sstable.Summary = buildSummary(conf, sstable.Index, generation, path, sstable.SingleFile)
	sstable.Filter = buildBloomFilter(conf, generation, path, sstable.Data, sstable.SingleFile)
	sstable.Metadata = buildMetadata(generation, path, sstable.Data, sstable.SingleFile)
	if !sstable.SingleFile {
		dictPath := CreateFileName(path, generation, "Dictionary", "db")
		sstable.CompressionKey.Write(dictPath)
		// Upis TOC u fajl
		toc_path := CreateFileName(path, generation, "TOC", "txt")
		toc_data := fmt.Sprintf("Generation: %d\nData: %s\nIndex: %s\nSummary: %s\nFilter: %s\nMetadata: %s\n",
			sstable.Gen, CreateFileName(path, generation, "Data", "db"),
			CreateFileName(path, generation, "Index", "db"),
			CreateFileName(path, generation, "Summary", "db"),
			CreateFileName(path, generation, "Filter", "db"),
			CreateFileName(path, generation, "Metadata", "db"))
		WriteTxtToFile(toc_path, toc_data)
	} else {
		// Upisujemo sve u jedan fajl
		sstable.Data.DataFile.Offset = 0
		_, err := sstable.Data.WriteData(path, conf, sstable.CompressionKey)
		if err != nil {
			panic("Error writing data to file: " + err.Error())
		}
		sstable.Index.IndexFile.Offset = sstable.Data.DataFile.Offset + sstable.Data.DataFile.SizeOnDisk
		err = sstable.Index.WriteIndex(path, conf)
		if err != nil {
			panic("Error writing index to file: " + err.Error())
		}
		sstable.Summary.SummaryFile.Offset = sstable.Index.IndexFile.Offset + sstable.Index.IndexFile.SizeOnDisk
		err = sstable.Summary.WriteSummary(path, conf)
		if err != nil {
			panic("Error writing summary to file: " + err.Error())
		}
		filterOffset := sstable.Summary.SummaryFile.Offset + sstable.Summary.SummaryFile.SizeOnDisk
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			return nil
		}
		defer file.Close()
		offset, err := file.Seek(0, io.SeekEnd) // Pozicioniramo se na kraj fajla
		if err != nil {
			return nil
		}
		serializedFilter := sstable.Filter.Serialize()
		_, err = file.Write(serializedFilter) // Upisujemo podatke na kraj fajla
		if err != nil {
			return nil
		}
		metadataOffset := offset + int64(len(serializedFilter))
		serializedMetadata := sstable.Metadata.Serialize()
		_, err = file.Write(serializedMetadata) // Upisujemo podatke na kraj fajla
		if err != nil {
			return nil
		}
		compressionOffset := metadataOffset + int64(len(serializedMetadata))
		serializedCompression := sstable.CompressionKey.Serialize()
		_, err = file.Write(serializedCompression) // Upisujemo podatke na kraj fajla
		if err != nil {
			return nil
		}
		// Upisujemo TOC u fajl
		offsets := make(map[string]int64)
		offsets["Data"] = sstable.Data.DataFile.Offset
		offsets["Index"] = sstable.Index.IndexFile.Offset
		offsets["Summary"] = sstable.Summary.SummaryFile.Offset
		offsets["Filter"] = filterOffset
		offsets["Metadata"] = metadataOffset
		offsets["Compression"] = compressionOffset
		_, err = file.Write([]byte("TOC\n"))
		if err != nil {
			return nil
		}
		for key, offset := range offsets {
			_, err = file.Write([]byte(fmt.Sprintf("%s: %d\n", key, offset)))
			if err != nil {
				return nil
			}
		}
	}

	sstable.Dir = path
	return &sstable
}

func buildData(mem memtable.Memtable, conf *config.Config, gen int, path string, singleFile bool) (*Data, *compression.Dictionary) {
	db := &Data{}
	dict := compression.NewDictionary()
	for i := 0; i < mem.Capacity; i++ {
		entry, found := mem.Structure.Search(mem.Keys[i])
		if found {
			dr := NewDataRecord(entry.Key, entry.Value, entry.Timestamp, entry.Tombstone)
			db.Records = append(db.Records, dr)
			if conf.SSTable.UseCompression {
				dict.Add(entry.Key)
			}
		}
	}
	if !singleFile {
		filename := CreateFileName(path, gen, "Data", "db")
		if conf.SSTable.UseCompression {
			db.WriteData(filename, conf, dict)
		} else {
			db.WriteData(filename, conf, nil)
		}
	}
	return db, dict
}

func buildIndex(conf *config.Config, gen int, path string, db *Data, singleFile bool) *Index {
	ib := &Index{}
	filename := CreateFileName(path, gen, "Index", "db")
	for _, record := range db.Records {
		ir := NewIndexRecord(record.Key, record.Offset)
		ib.Records = append(ib.Records, ir)
	}
	if !singleFile {
		err := ib.WriteIndex(filename, conf)
		if err != nil {
			panic("Error writing index to file: " + err.Error())
		}
	}
	return ib
}

func buildSummary(conf *config.Config, index *Index, gen int, path string, singleFile bool) *Summary {
	sb := &Summary{}
	filename := CreateFileName(path, gen, "Summary", "db")
	for i := 0; i < len(index.Records); i += conf.SSTable.SummaryLevel {
		if i+conf.SSTable.SummaryLevel >= len(index.Records) {
			// Pravimo summary sa onolko koliko je ostalo
			sr := SummaryRecord{
				FirstKey:        index.Records[i].Key,
				IndexOffset:     index.Records[i].IndexOffset,
				NumberOfRecords: len(index.Records) - i,
			}
			sb.Records = append(sb.Records, sr)
			break
		} else {
			sr := SummaryRecord{
				FirstKey:        index.Records[i].Key,
				IndexOffset:     index.Records[i].IndexOffset,
				NumberOfRecords: conf.SSTable.SummaryLevel,
			}
			sb.Records = append(sb.Records, sr)
		}
	}

	sb.FirstKey = sb.Records[0].FirstKey
	sb.LastKey = index.Records[len(index.Records)-1].Key
	if !singleFile {
		err := sb.WriteSummary(filename, conf)
		if err != nil {
			panic("Error writing summary to file: " + err.Error())
		}
	}
	return sb
}

func buildBloomFilter(conf *config.Config, gen int, path string, db *Data, singleFile bool) bloomfilter.BloomFilter {
	filename := CreateFileName(path, gen, "Filter", "db")
	fb := bloomfilter.MakeBloomFilter(len(db.Records), 0.5)
	for _, record := range db.Records {
		fb.Add(record.Key)
		if !record.Tombstone {
			fb.Add(record.Value)
		}
	}
	if !singleFile {
		serialized := fb.Serialize()
		bm := block_organization.NewBlockManager(conf)
		_, err := bm.AppendBlock(filename, serialized)
		if err != nil {
			panic("Error writing bloom filter to file: " + err.Error())
		}
	}
	return fb
}

// buildMetadata kreira Merkle stablo i upisuje ga u fajl
func buildMetadata(gen int, path string, db *Data, singleFile bool) *merkle.MerkleTree {
	filename := CreateFileName(path, gen, "Metadata", "db")
	data := make([][]byte, len(db.Records))
	for i, record := range db.Records {
		data[i] = record.Key
		if !record.Tombstone {
			data[i] = append(data[i], record.Value...)
		}
	}
	mt := merkle.NewMerkleTree(data)
	if !singleFile {
		err := mt.SerializeToBinaryFile(filename)
		if err != nil {
			panic("Error writing Merkle tree to file: " + err.Error())
		}
	}
	return mt
}

func ReadBloomFilter(path string, conf *config.Config) (bloomfilter.BloomFilter, error) {
	bm := block_organization.NewBlockManager(conf) // Koristimo nil jer nam nije potreban config ovde
	block, err := bm.ReadBlock(path, 0)
	if err != nil {
		return bloomfilter.BloomFilter{}, fmt.Errorf("error reading bloom filter from file %s: %w", path, err)
	}
	fb := bloomfilter.Deserialize(block)
	return fb[0], nil
}

func ReadMetadata(path string) (*merkle.MerkleTree, error) {
	mt, err := merkle.DeserializeFromBinaryFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading Merkle tree from file %s: %w", path, err)
	}
	return mt, nil
}

func ReadSingleFileSSTable(sstable *SSTable, dir string, gen int, conf *config.Config) *SSTable {
	//
	return sstable
}

// NewSSTable kreira novi SSTable iz fajlova
func NewSSTable(dir string, conf *config.Config, gen int) *SSTable {
	sstable := &SSTable{
		CompressionKey: compression.NewDictionary(),
		Gen:            gen,
	}
	dir = fmt.Sprintf("%s/%d", dir, gen)
	// Proveravamo da li direktorijum postoji
	//err := CreateDirectoryIfNotExists(dir)
	if conf.SSTable.SingleFile {
		sstable = ReadSingleFileSSTable(sstable, dir, gen, conf)
		return sstable
	}

	// Citanje kompresije iz fajla
	dictPath := CreateFileName(dir, gen, "Dictionary", "db")
	dict, err := compression.Read(dictPath)
	if err != nil {
		panic("Error reading dictionary from file: " + err.Error())
	}
	println("Dictionary read from file:", dictPath)
	sstable.UseCompression = true
	sstable.CompressionKey = dict
	if dict != nil {
		if dict.IsEmpty() {
			sstable.UseCompression = false
			sstable.CompressionKey = nil

		} else {
			//dict.Print()
		}
	} else {
		sstable.UseCompression = false
		sstable.CompressionKey = nil
	}

	println("Using compression:", sstable.UseCompression)
	sstable.Dir = dir
	// Citanje Data fajla
	dataPath := CreateFileName(dir, gen, "Data", "db")
	data, err := ReadData(dataPath, conf, sstable.CompressionKey)
	if err != nil {
		panic("Error reading data from file: " + err.Error())
	}
	sstable.Data = data
	// Citanje Index fajla
	indexPath := CreateFileName(dir, gen, "Index", "db")
	index, err := ReadIndex(indexPath, conf)
	if err != nil {
		panic("Error reading index from file: " + err.Error())
	}
	sstable.Index = index
	// Citanje Summary fajla
	summaryPath := CreateFileName(dir, gen, "Summary", "db")
	summary, err := ReadSummary(summaryPath, conf)
	if err != nil {
		panic("Error reading summary from file: " + err.Error())
	}
	sstable.Summary = summary

	// Citanje Filter fajla
	filterPath := CreateFileName(dir, gen, "Filter", "db")
	filterData, err := ReadBloomFilter(filterPath, conf)
	if err != nil {
		panic("Error reading bloom filter from file: " + err.Error())
	}
	sstable.Filter = filterData
	// Citanje Metadata fajla
	metadataPath := CreateFileName(dir, gen, "Metadata", "db")
	metadata, err := ReadMetadata(metadataPath)
	if err != nil {
		panic("Error reading Merkle tree from file: " + err.Error())
	}
	sstable.Metadata = metadata

	return sstable
}

// WriteSSTable upisuje SSTable u fajl
// Pomocna funkcija za LSM
func WriteSSTable(sstable *SSTable, dir string, conf *config.Config) error {
	path := fmt.Sprintf("%s/%d", dir, sstable.Gen)
	err := CreateDirectoryIfNotExists(path)
	if err != nil {
		return fmt.Errorf("error creating directory for SSTable: %w", err)
	}

	// Write Data
	dataPath := CreateFileName(path, sstable.Gen, "Data", "db")
	_, err = sstable.Data.WriteData(dataPath, conf, sstable.CompressionKey)
	if err != nil {
		return fmt.Errorf("error writing data to file %s: %w", dataPath, err)
	}

	// Write Index
	indexPath := CreateFileName(path, sstable.Gen, "Index", "db")
	err = sstable.Index.WriteIndex(indexPath, conf)
	if err != nil {
		return fmt.Errorf("error writing index to file %s: %w", indexPath, err)
	}

	// Write Summary
	summaryPath := CreateFileName(path, sstable.Gen, "Summary", "db")
	err = sstable.Summary.WriteSummary(summaryPath, conf)
	if err != nil {
		return fmt.Errorf("error writing summary to file %s: %w", summaryPath, err)
	}

	// Write Bloom Filter
	filterPath := CreateFileName(path, sstable.Gen, "Filter", "db")
	bm := block_organization.NewBlockManager(conf)
	_, err = bm.AppendBlock(filterPath, sstable.Filter.Serialize())
	if err != nil {
		return fmt.Errorf("error writing bloom filter to file %s: %w", filterPath, err)
	}

	// Write Compression Dictionary
	dictPath := CreateFileName(dir, sstable.Gen, "Dictionary", "db")
	sstable.CompressionKey.Write(dictPath)

	// Write Metadata
	metadataPath := CreateFileName(path, sstable.Gen, "Metadata", "db")
	err = sstable.Metadata.SerializeToBinaryFile(metadataPath)
	if err != nil {
		return fmt.Errorf("error writing Merkle tree to file %s: %w", metadataPath, err)
	}
	// Write TOC file
	tocPath := CreateFileName(path, sstable.Gen, "TOC", "txt")
	tocData := fmt.Sprintf("Generation: %d\nData: %s\nIndex: %s\nSummary: %s\nFilter: %s\nMetadata: %s\n",
		sstable.Gen, CreateFileName(path, sstable.Gen, "Data", "db"),
		CreateFileName(path, sstable.Gen, "Index", "db"),
		CreateFileName(path, sstable.Gen, "Summary", "db"),
		CreateFileName(path, sstable.Gen, "Filter", "db"),
		CreateFileName(path, sstable.Gen, "Metadata", "db"))
	err = WriteTxtToFile(tocPath, tocData)
	if err != nil {
		return fmt.Errorf("error writing TOC to file %s: %w", tocPath, err)
	}

	return nil
}

// NewEmptySSTable kreira prazan SSTable
// Pomocna funkcija za LSM
func NewEmptySSTable(conf *config.Config, generation int) *SSTable {
	sstable := &SSTable{
		Data:           &Data{Records: []DataRecord{}},
		Index:          &Index{Records: []IndexRecord{}},
		Summary:        &Summary{Records: []SummaryRecord{}},
		Gen:            generation,
		UseCompression: conf.SSTable.UseCompression,
		CompressionKey: compression.NewDictionary(),
	}
	if sstable.UseCompression {
		sstable.CompressionKey = compression.NewDictionary()
	}
	return sstable
}

// Pomocna funkcija za PreffixIterate i RangeIterate
// Trazi prvi sledeci zapis koji pocinje sa prefiksom/key-em
// Prvo trazi u Summary, onda u Indexu, a zatim u Data segmentu
func (s *SSTable) ReadRecordWithKey(bm *block_organization.BlockManager, blockNumber int, prefix string, rangeIter bool) (adapter.MemtableEntry, int) {

	sumRec, err := s.Summary.FindSummaryRecordWithKey(prefix) // Prvo trazimo u Summary
	if err != nil {
		return adapter.MemtableEntry{}, -1
	}
	// Ako smo nasli u Summaryu, trazimo njegov offset u Data fajlu u Indexu
	dataOffset := -1
	if rangeIter {
		dataOffset, err = s.Index.FindDataOffsetWithKey(sumRec.IndexOffset, []byte(prefix), bm)
		if err != nil {
			return adapter.MemtableEntry{}, -1
		}
	} else {
		dataOffset, err = s.Index.FindDataOffsetWithPrefix(sumRec.IndexOffset, []byte(prefix), bm)
		if err != nil {
			println("Error finding data offset with key:", prefix, "Error:", err)
			return adapter.MemtableEntry{}, -1
		}
	}
	dataRec, nextBlock := s.Data.ReadRecord(bm, dataOffset/bm.BlockSize, s.CompressionKey) // Citanje iz Data fajla
	if dataRec.Key == nil {
		return adapter.MemtableEntry{}, -1
	}
	return adapter.MemtableEntry{
		Key:       dataRec.Key,
		Value:     dataRec.Value,
		Timestamp: dataRec.Timestamp,
		Tombstone: dataRec.Tombstone,
	}, nextBlock
}

func StartSSTable(gen int, conf *config.Config) (*SSTable, error) {
	// Ucitavamo bloom filter i summary iz fajla
	if gen < 1 {
		return nil, fmt.Errorf("invalid generation number: %d", gen)
	}
	dir := fmt.Sprintf("%s/%d", conf.SSTable.SstableDirectory, gen)
	// Ucitavamo BloomFiler
	bfPath := CreateFileName(dir, gen, "Filter", "db")
	bf, err := ReadBloomFilter(bfPath, conf)
	if err != nil {
		return nil, fmt.Errorf("error reading bloom filter: %w", err)
	}
	// Ucitavamo Summary
	summaryPath := CreateFileName(dir, gen, "Summary", "db")
	summary, err := ReadSummary(summaryPath, conf)
	if err != nil {
		return nil, fmt.Errorf("error reading summary: %w", err)
	}
	// Ucitavamo compression
	compressionPath := CreateFileName(dir, gen, "Dictionary", "db")
	dict, err := compression.Read(compressionPath)
	if err != nil {
		return nil, fmt.Errorf("error reading compression dictionary: %w", err)
	}
	useCompression := true
	if dict != nil {
		if dict.IsEmpty() {
			useCompression = false
			dict = nil
		} else {
			dict.Print()
		}
	} else {
		useCompression = false
		dict = nil
	}

	data := &Data{
		DataFile: File{
			Path:       CreateFileName(dir, gen, "Data", "db"),
			Offset:     0,
			SizeOnDisk: 0,
		},
		Records: []DataRecord{},
	}
	index := &Index{
		IndexFile: File{
			Path:       CreateFileName(dir, gen, "Index", "db"),
			Offset:     0,
			SizeOnDisk: 0,
		},
		Records: []IndexRecord{},
	}

	sstable := &SSTable{
		Data:           data,
		Index:          index,
		Summary:        summary,
		Gen:            gen,
		UseCompression: useCompression,
		CompressionKey: dict,
		Filter:         bf,
		Metadata:       &merkle.MerkleTree{},
		Dir:            dir,
	}
	return sstable, nil
}
