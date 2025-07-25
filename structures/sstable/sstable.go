package sstable

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

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
	Level          int // Nivo u LSM stablu
	Gen            int
	UseCompression bool
	CompressionKey *compression.Dictionary
	Dir            string
	SingleFile     bool  // Da li se SSTable cuva u jednom fajlu ili u vise
	FilterOffset   int64 // Offset Bloom filtera u fajlu
	MetadataOffset int64 // Offset Merkle stabla u fajlu
}

// NewSSTable kreira novi SSTable
func FlushSSTable(conf *config.Config, memtable memtable.Memtable, generation int, dict *compression.Dictionary) *SSTable {
	//Sortiramo memtable.Keys da bismo imali uredjen redosled
	sort.Slice(memtable.Keys, func(i, j int) bool {
		return bytes.Compare(memtable.Keys[i], memtable.Keys[j]) < 0
	})

	var sstable SSTable
	sstable.UseCompression = conf.SSTable.UseCompression
	sstable.CompressionKey = dict

	if sstable.UseCompression && (dict == nil || dict.IsEmpty()) {
		println("WARNING: Compression enabled but dictionary is nil/empty, disabling compression")
		sstable.UseCompression = false
		sstable.CompressionKey = nil
	}

	sstable.Level = 1 // Postavljamo nivo na 1, jer se Memtable flushuje kao SSTable na prvi nivo
	sstable.Gen = generation
	path := fmt.Sprintf("%s/%d/%d", conf.SSTable.SstableDirectory, sstable.Level, sstable.Gen)
	err := CreateDirectoryIfNotExists(path)
	if err != nil {
		panic("Error creating directory for SSTable: " + err.Error())
	}

	sstable.SingleFile = conf.SSTable.SingleFile
	if sstable.SingleFile {
		bm := block_organization.NewBlockManager(conf)
		fm := CreateFileName(path, generation, "SSTable", "db")
		_, err := bm.AppendBlock(fm, []byte("TOC"))
		if err != nil {
			panic("Error creating single file SSTable: " + err.Error())
		}
	}
	sstable.Data = buildData(memtable, conf, generation, path, sstable.SingleFile, dict)
	sstable.Index = buildIndex(conf, generation, path, sstable.Data, sstable.SingleFile)
	sstable.Summary = buildSummary(conf, sstable.Index, generation, path, sstable.SingleFile)
	sstable.Filter = buildBloomFilter(conf, generation, path, sstable.Data, sstable.SingleFile)
	sstable.Metadata = buildMetadata(generation, path, sstable.Data, sstable.SingleFile, conf)
	if !sstable.SingleFile {
		dictPath := CreateFileName(path, generation, "CompressionInfo", "db")
		// Upisujemo true ili false u fajl da li koristimo kompresiju
		sstable.WriteCompressionInfo(dictPath, dict, conf)
		// Upis TOC u fajl
		toc_path := CreateFileName(path, generation, "TOC", "txt")
		toc_data := fmt.Sprintf("Generation: %d\nData: %s\nIndex: %s\nSummary: %s\nFilter: %s\nMetadata: %s\nCompression: %s\n",
			sstable.Gen, CreateFileName(path, generation, "Data", "db"),
			CreateFileName(path, generation, "Index", "db"),
			CreateFileName(path, generation, "Summary", "db"),
			CreateFileName(path, generation, "Filter", "db"),
			CreateFileName(path, generation, "Metadata", "db"),
			CreateFileName(path, generation, "CompressionInfo", "db"))
		WriteTxtToFile(toc_path, toc_data)
	} else {
		// Upisujemo sve u jedan fajl
		path = CreateFileName(path, generation, "SSTable", "db")
		if err := sstable.WriteSingleFile(path, conf); err != nil {
			fmt.Printf("Error writing single file SSTable: %v\n", err)
		}
	}

	sstable.Dir = path
	return &sstable
}

func (sstable *SSTable) WriteSingleFile(path string, conf *config.Config) error {
	sstable.Data.DataFile.Offset = int64(1 * conf.Block.BlockSize)
	sstable.Data.DataFile.SizeOnDisk = sstable.Index.IndexFile.Offset - sstable.Data.DataFile.Offset
	sstable.Index.IndexFile.SizeOnDisk = sstable.Summary.SummaryFile.Offset - sstable.Index.IndexFile.Offset
	bm := block_organization.NewBlockManager(conf)
	serializedFilter := sstable.Filter.Serialize()
	bn, err := bm.AppendBlock(path, serializedFilter)
	if err != nil {
		return fmt.Errorf("error writing filter to file: %w", err)
	}
	filterOffset := int64(bn * bm.BlockSize)

	metadata, err := sstable.Metadata.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing metadata: %w", err)
	}
	bn, err = bm.AppendBlock(path, metadata)
	if err != nil {
		return fmt.Errorf("error writing metadata to file: %w", err)
	}
	metadataOffset := int64(bn * bm.BlockSize)

	var compressionByte []byte
	if sstable.UseCompression {
		compressionByte = []byte("Using compression")
	} else {
		compressionByte = []byte("No compression")
	}
	bn, err = bm.AppendBlock(path, compressionByte)
	if err != nil {
		return fmt.Errorf("error writing compression info: %w", err)
	}
	compressionOffset := int64(bn * bm.BlockSize)

	// Upisujemo TOC u fajl
	offsets := make(map[string]int64)
	offsets["Data"] = sstable.Data.DataFile.Offset
	offsets["Index"] = sstable.Index.IndexFile.Offset
	offsets["Summary"] = sstable.Summary.SummaryFile.Offset
	offsets["Filter"] = filterOffset
	offsets["Metadata"] = metadataOffset
	offsets["Compression"] = compressionOffset
	_, err = bm.AppendBlock(path, []byte("TOC\n"))
	if err != nil {
		return fmt.Errorf("error writing TOC header: %w", err)
	}
	serializedToc := []byte{}
	for key, offset := range offsets {
		println("Writing TOC entry:", key, "at offset", offset)
		serializedToc = append(serializedToc, []byte(fmt.Sprintf("%s: %d\n", key, offset))...)
	}
	err = bm.WriteBlock(path, 0, serializedToc)
	if err != nil {
		return fmt.Errorf("error writing TOC entries: %w", err)
	}
	return nil
}

func (s *SSTable) WriteCompressionInfo(path string, dict *compression.Dictionary, conf *config.Config) {
	bm := block_organization.NewBlockManager(conf)
	data := []byte{0}
	if s.UseCompression && dict != nil && !dict.IsEmpty() {
		data[0] = 1
	} else {
		data[0] = 0
	}
	_, err := bm.AppendBlock(path, data)
	if err != nil {
		panic("Error writing compression info to file: " + err.Error())
	}
}

func ReadCompressionInfo(path string, conf *config.Config) (bool, error) {
	bm := block_organization.NewBlockManager(conf)
	block, err := bm.ReadBlock(path, 0)
	if err != nil {
		return false, fmt.Errorf("error reading compression info from file %s: %w", path, err)
	}
	return block[0] == 1, nil
}

func buildData(mem memtable.Memtable, conf *config.Config, gen int, path string, singleFile bool, dict *compression.Dictionary) *Data {
	db := &Data{}
	for i := 0; i < mem.Capacity; i++ {
		entry, found := mem.Structure.Search(mem.Keys[i])
		if found {
			dr := NewDataRecord(entry.Key, entry.Value, entry.Timestamp, entry.Tombstone)
			db.Records = append(db.Records, dr)
		}
	}
	filename := CreateFileName(path, gen, "SSTable", "db")
	if !singleFile {
		filename = CreateFileName(path, gen, "Data", "db")
	}
	if conf.SSTable.UseCompression {
		db.WriteData(filename, conf, dict)
	} else {
		db.WriteData(filename, conf, nil)
	}
	return db
}

func buildIndex(conf *config.Config, gen int, path string, db *Data, singleFile bool) *Index {
	ib := &Index{}
	for _, record := range db.Records {
		ir := NewIndexRecord(record.Key, record.Offset)
		ib.Records = append(ib.Records, ir)
	}

	filename := CreateFileName(path, gen, "SSTable", "db")
	if !singleFile {
		filename = CreateFileName(path, gen, "Index", "db")
	}
	err := ib.WriteIndex(filename, conf)
	if err != nil {
		panic("Error writing index to file: " + err.Error())
	}
	return ib
}

func buildSummary(conf *config.Config, index *Index, gen int, path string, singleFile bool) *Summary {
	sb := &Summary{}
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

	filename := CreateFileName(path, gen, "SSTable", "db")
	sb.FirstKey = sb.Records[0].FirstKey
	sb.LastKey = index.Records[len(index.Records)-1].Key
	if !singleFile {
		filename = CreateFileName(path, gen, "Summary", "db")
	}
	err := sb.WriteSummary(filename, conf)
	if err != nil {
		panic("Error writing summary to file: " + err.Error())
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
func buildMetadata(gen int, path string, db *Data, singleFile bool, conf *config.Config) *merkle.MerkleTree {
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
		serialized, _ := mt.Serialize()
		bm := block_organization.NewBlockManager(conf)
		_, err := bm.AppendBlock(filename, serialized)
		if err != nil {
			panic("Error writing bloom filter to file: " + err.Error())
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

func ReadMetadata(path string, conf *config.Config) (*merkle.MerkleTree, error) {
	m := &merkle.MerkleTree{}
	bm := block_organization.NewBlockManager(conf)
	block, err := bm.ReadBlock(path, 0)
	if err != nil {
		return nil, fmt.Errorf("error reading Merkle tree from file %s: %w", path, err)
	}
	m, err = merkle.Deserialize(block)
	return m, nil
}

// Ucitava offsete sa kraja fajla
func ReadOffsetsFromFile(path string, conf *config.Config) (map[string]int64, error) {
	offsets := make(map[string]int64)
	bm := block_organization.NewBlockManager(conf)
	block, err := bm.ReadBlock(path, 0)
	if err != nil {
		return nil, fmt.Errorf("error reading offsets from file %s: %w", path, err)
	}
	lines := strings.Split(string(block), "\n")
	for _, line := range lines {
		parts := strings.Split(line, ": ")
		if len(parts) == 2 {
			val, err := strconv.ParseInt(parts[1], 10, 64)
			if err == nil {
				offsets[parts[0]] = val
			}
		}
	}
	if len(offsets) == 0 {
		return nil, fmt.Errorf("no offsets found in file %s", path)
	}
	return offsets, nil
}

func (sstable *SSTable) ReadFilterMetaCompression(path string, offsets map[string]int64, readMerkle bool, conf *config.Config) error {
	//Citanje Compression info
	bm := block_organization.NewBlockManager(conf)
	block, err := bm.ReadBlock(path, int(offsets["Compression"]/int64(conf.Memtable.NumberOfMemtables)))
	if err != nil {
		return fmt.Errorf("error reading compression info from file: %w", err)
	}
	UsingCompression := string(block) == "Using compression"
	sstable.UseCompression = UsingCompression

	// Citanje Bloom filtera
	block, err = bm.ReadBlock(path, int(offsets["Filter"]/int64(conf.Memtable.NumberOfMemtables)))
	if err != nil {
		return fmt.Errorf("error reading Bloom filter from file: %w", err)
	}
	sstable.Filter = bloomfilter.Deserialize(block)[0]
	if readMerkle {
		// Citanje Merkle stabla
		block, err = bm.ReadBlock(path, int(offsets["Metadata"]/int64(conf.Memtable.NumberOfMemtables)))
		if err != nil {
			return fmt.Errorf("error reading Merkle tree from file: %w", err)
		}
		sstable.Metadata, err = merkle.Deserialize(block)
		if err != nil {
			return fmt.Errorf("error deserializing Merkle tree: %w", err)
		}
	}
	return nil
}

// NewSSTable kreira novi SSTable iz fajlova
func NewSSTable(conf *config.Config, level int, gen int, dict *compression.Dictionary) *SSTable {
	sstable := &SSTable{
		CompressionKey: dict,
		Gen:            gen,
		Level:          level,
		SingleFile:     conf.SSTable.SingleFile,
	}
	dir := fmt.Sprintf("%s/%d/%d", conf.SSTable.SstableDirectory, level, gen)
	// Proveravamo da li direktorijum postoji
	//err := CreateDirectoryIfNotExists(dir)
	//Ucitavamo offsete sa kraja fajla ako je SingleFile
	if conf.SSTable.SingleFile {
		path := CreateFileName(dir, gen, "SSTable", "db")
		offsets, err := ReadOffsetsFromFile(path, conf)
		if err != nil {
			panic("Error reading offsets from file: " + err.Error())
		}
		sstable.Data = &Data{
			DataFile: File{
				Offset: offsets["Data"],
			},
		}
		sstable.Index = &Index{
			IndexFile: File{
				Offset: offsets["Index"],
			},
		}
		sstable.Summary = &Summary{
			SummaryFile: File{
				Offset: offsets["Summary"],
			},
		}
		sstable.FilterOffset = offsets["Filter"]
		println("Offsets read from file:")
		for key, offset := range offsets {
			println("Offset for", key, "is", offset)
		}

		// Citamo compression info, bloom filter i Merkle tree
		err = sstable.ReadFilterMetaCompression(path, offsets, true, conf)
		if err != nil {
			panic("Error reading filter, metadata and compression info: " + err.Error())
		}
		if sstable.UseCompression {
			sstable.CompressionKey = dict
		} else {
			sstable.CompressionKey = nil
		}

	}

	if !sstable.SingleFile {
		// Citanje kompresije iz fajla
		dictPath := CreateFileName(dir, gen, "CompressionInfo", "db")
		useCompression, err := ReadCompressionInfo(dictPath, conf)
		if err != nil {
			panic("Error reading compression info from file: " + err.Error())
		}
		sstable.UseCompression = useCompression
		sstable.CompressionKey = dict
		if !useCompression {
			sstable.CompressionKey = nil
		}

		println("Using compression:", sstable.UseCompression)
	}

	sstable.Dir = dir
	// Citanje Data fajla
	dataPath := CreateFileName(dir, gen, "SSTable", "db")
	if !sstable.SingleFile {
		dataPath = CreateFileName(dir, gen, "Data", "db")
		data, err := ReadData(dataPath, conf, sstable.CompressionKey, 0, 0)
		if err != nil {
			panic("Error reading data from file: " + err.Error())
		}
		sstable.Data = data
	} else {
		data, err := ReadData(dataPath, conf, sstable.CompressionKey, sstable.Data.DataFile.Offset, sstable.Index.IndexFile.Offset)
		if err != nil {
			panic("Error reading data from file: " + err.Error())
		}
		sstable.Data = data
	}
	// Citanje Index fajla
	indexPath := dataPath
	if !sstable.SingleFile {
		indexPath = CreateFileName(dir, gen, "Index", "db")
		index, err := ReadIndex(indexPath, conf, 0, 0)
		if err != nil {
			panic("Error reading index from file: " + err.Error())
		}
		sstable.Index = index
	} else {
		index, err := ReadIndex(indexPath, conf, sstable.Index.IndexFile.Offset, sstable.Summary.SummaryFile.Offset)
		if err != nil {
			panic("Error reading index from file: " + err.Error())
		}
		sstable.Index = index
	}

	// Citanje Summary fajla
	summaryPath := indexPath
	if !sstable.SingleFile {
		summaryPath = CreateFileName(dir, gen, "Summary", "db")
		summary, err := ReadSummary(summaryPath, conf, 0, 0)
		if err != nil {
			panic("Error reading summary from file: " + err.Error())
		}
		sstable.Summary = summary
	} else {
		summary, err := ReadSummary(summaryPath, conf, sstable.Summary.SummaryFile.Offset, sstable.FilterOffset)
		if err != nil {
			panic("Error reading summary from file: " + err.Error())
		}
		sstable.Summary = summary
	}
	if !sstable.SingleFile {
		// Citanje Filter fajla
		filterPath := CreateFileName(dir, gen, "Filter", "db")
		filterData, err := ReadBloomFilter(filterPath, conf)
		if err != nil {
			panic("Error reading bloom filter from file: " + err.Error())
		}
		sstable.Filter = filterData
		// Citanje Metadata fajla
		metadataPath := CreateFileName(dir, gen, "Metadata", "db")
		metadata, err := ReadMetadata(metadataPath, conf)
		if err != nil {
			panic("Error reading Merkle tree from file: " + err.Error())
		}
		sstable.Metadata = metadata
	}

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
	_, err = sstable.Metadata.SerializeToBinaryFile(metadataPath, 0)
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
func NewEmptySSTable(conf *config.Config, level int, generation int) *SSTable {
	sstable := &SSTable{
		Data:           &Data{Records: []DataRecord{}},
		Index:          &Index{Records: []IndexRecord{}},
		Summary:        &Summary{Records: []SummaryRecord{}},
		Gen:            generation,
		Level:          level,
		UseCompression: conf.SSTable.UseCompression,
		CompressionKey: compression.NewDictionary(),
	}
	if sstable.UseCompression {
		sstable.CompressionKey = compression.NewDictionary()
	}
	return sstable
}

// Get traži ključ u SSTable-u i vraća odgovarajući DataRecord
// Pomocna funkcija za LSM
// TODO: Dodati SingleFile opciju
func (s *SSTable) Get(conf *config.Config, key []byte) (*DataRecord, error) {
	// Proveri Bloom filter pre pretrage
	if !s.Filter.Read(key) {
		return nil, nil
	}

	// Ako Bloom filter sadrži ključ, proveri u summary i index
	for _, summary := range s.Summary.Records {
		if bytes.Compare(key, summary.FirstKey) < 0 {
			continue // Ključ nije u ovom summary bloku
		}

		// Ako je ključ unutar opsega summary, proveri index
		// indexOffset je offset u Index segmentu gde se nalazi ovaj summary
		start := summary.IndexOffset
		end := start + summary.NumberOfRecords

		indexBlock := s.Index.Records[start:end]

		// Binarno pretraži ključ u indexBlock-u
		i := sort.Search(len(indexBlock), func(i int) bool {
			return bytes.Compare(indexBlock[i].Key, key) >= 0
		})

		if i < len(indexBlock) && bytes.Equal(indexBlock[i].Key, key) {
			// Ako je ključ pronađen u indexu, pročitaj podatke iz Data segmenta
			dataOffset := indexBlock[i].Offset
			dir := fmt.Sprintf("%s/%d/%d", conf.SSTable.SstableDirectory, s.Level, s.Gen)
			filename := CreateFileName(dir, s.Gen, "Data", "db")

			record, err := s.Data.ReadRecordAtOffset(filename, conf, s.CompressionKey, dataOffset)
			if err != nil {
				return nil, err
			}

			if record.Tombstone {
				return nil, fmt.Errorf("record marked as deleted") // Ako je tombstone, ključ je obrisan
			}

			return record, nil
		}
	}

	return nil, nil
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

func StartSSTable(level int, gen int, conf *config.Config, dict *compression.Dictionary) (*SSTable, error) {
	// Ucitavamo bloom filter i summary iz fajla
	if gen < 1 {
		return nil, fmt.Errorf("invalid generation number: %d", gen)
	}
	dir := fmt.Sprintf("%s/%d/%d", conf.SSTable.SstableDirectory, level, gen)

	if conf.SSTable.SingleFile {
		sstable := &SSTable{
			Gen:        gen,
			Level:      level,
			SingleFile: conf.SSTable.SingleFile,
			Dir:        dir,
		}
		path := CreateFileName(dir, gen, "SSTable", "db")
		offsets, err := ReadOffsetsFromFile(path, conf)
		if err != nil {
			panic("Error reading offsets from file: " + err.Error())
		}
		sstable.Data = &Data{
			DataFile: File{
				Path:       path,
				Offset:     offsets["Data"],
				SizeOnDisk: offsets["Index"] - offsets["Data"],
			},
		}
		sstable.Index = &Index{
			IndexFile: File{
				Path:       path,
				Offset:     offsets["Index"],
				SizeOnDisk: offsets["Summary"] - offsets["Index"],
			},
		}
		sstable.Summary = &Summary{
			SummaryFile: File{
				Path:       path,
				Offset:     offsets["Summary"],
				SizeOnDisk: offsets["Filter"] - offsets["Summary"],
			},
		}
		sstable.FilterOffset = offsets["Filter"]
		sstable.MetadataOffset = offsets["Metadata"]

		// Citamo compression info, bloom filter
		err = sstable.ReadFilterMetaCompression(path, offsets, false, conf)
		if err != nil {
			panic("Error reading filter, metadata and compression info: " + err.Error())
		}
		if sstable.UseCompression {
			sstable.CompressionKey = dict
		} else {
			sstable.CompressionKey = nil
		}
		//Ucitavamo Summary
		summary, err := ReadSummary(sstable.Summary.SummaryFile.Path, conf, sstable.Summary.SummaryFile.Offset, sstable.FilterOffset)
		if err != nil {
			return nil, fmt.Errorf("error reading summary: %w", err)
		}
		sstable.Summary = summary
		return sstable, nil

	}
	// Ucitavamo BloomFiler
	bfPath := CreateFileName(dir, gen, "Filter", "db")
	bf, err := ReadBloomFilter(bfPath, conf)
	if err != nil {
		return nil, fmt.Errorf("error reading bloom filter: %w", err)
	}
	// Ucitavamo Summary
	summaryPath := CreateFileName(dir, gen, "Summary", "db")
	summary, err := ReadSummary(summaryPath, conf, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("error reading summary: %w", err)
	}
	dictpath := CreateFileName(dir, gen, "CompressionInfo", "db")
	UseCompression, err := ReadCompressionInfo(dictpath, conf)
	if err != nil {
		return nil, fmt.Errorf("error reading compression dictionary: %w", err)
	}
	dictionary := dict
	if !UseCompression {
		dictionary = nil // Ako ne koristimo kompresiju, dictionary je nil
	}

	data := &Data{
		DataFile: File{
			Path:       CreateFileName(dir, gen, "Data", "db"),
			Offset:     0,
			SizeOnDisk: -1,
		},
		Records: []DataRecord{},
	}
	index := &Index{
		IndexFile: File{
			Path:       CreateFileName(dir, gen, "Index", "db"),
			Offset:     0,
			SizeOnDisk: -1,
		},
		Records: []IndexRecord{},
	}
	sstable := &SSTable{
		Data:           data,
		Index:          index,
		Summary:        summary,
		Gen:            gen,
		Level:          level,
		UseCompression: UseCompression,
		CompressionKey: dictionary,
		Filter:         bf,
		Metadata:       &merkle.MerkleTree{},
		Dir:            dir,
		SingleFile:     conf.SSTable.SingleFile,
	}
	return sstable, nil
}

// TODO: Ispraviti ako je Dictionary globalan, a ne zaseban za SSTable
// DeleteFiles briše sve fajlove vezane za odgovarajući SSTable
func (s *SSTable) DeleteFiles(conf *config.Config) error {
	elements := []struct {
		element string
		ext     string
	}{
		{"Data", ".db"},
		{"Index", ".db"},
		{"Summary", ".db"},
		{"Filter", ".db"},
		{"Metadata", ".db"},
		{"Dictionary", ".db"},
		{"TOC", ".txt"},
	}
	path := fmt.Sprintf("%s/%d/%d", conf.SSTable.SstableDirectory, s.Level, s.Gen)

	for _, element := range elements {
		filePath := CreateFileName(path, s.Gen, element.element, element.ext)
		if FileExists(filePath) {
			if err := os.Remove(filePath); err != nil {
				return fmt.Errorf("failed to remove file %s: %w", filePath, err)
			}
		}
	}

	return nil
}

// ValidateMerkleTree proverava da li je doslo do izmene u podacima
// Ako jeste, vraca true, ako nije, vraca false
// Ako je doslo do greske u citanju podataka ili Merkle stabla, vraca gresku
func (sstable *SSTable) ValidateMerkleTree(conf *config.Config, dict *compression.Dictionary) (bool, error) {

	filename := CreateFileName(sstable.Dir, sstable.Gen, "Data", "db")
	if sstable.SingleFile {
		filename = CreateFileName(sstable.Dir, sstable.Gen, "SSTable", "db")
	}
	dict1 := dict
	if !sstable.UseCompression {
		dict1 = nil
	}
	db, err := ReadData(filename, conf, dict1, sstable.Data.DataFile.Offset, sstable.Index.IndexFile.Offset)
	if err != nil {
		return false, fmt.Errorf("error reading data: %w", err)
	}
	data := make([][]byte, len(db.Records))
	for i, record := range db.Records {
		data[i] = record.Key
		if !record.Tombstone {
			data[i] = append(data[i], record.Value...)
		}
	}
	new_mt := merkle.NewMerkleTree(data)
	if !sstable.SingleFile {
		filename = CreateFileName(sstable.Dir, sstable.Gen, "Metadata", "db")

	}
	bm := block_organization.NewBlockManager(conf)
	bn := 0
	if sstable.SingleFile {
		bn = int(sstable.MetadataOffset / int64(bm.BlockSize))
	}
	block, err := bm.ReadBlock(filename, bn)
	if err != nil {
		return false, fmt.Errorf("error reading block from file %s: %w", filename, err)
	}
	old_mt, err := merkle.Deserialize(block)
	if err != nil {
		return false, fmt.Errorf("error reading Merkle tree from file %s: %w", filename, err)
	}
	if old_mt.MerkleRootHash == new_mt.MerkleRootHash {
		println("There hasn't been any changes in the data, Merkle tree is valid")
		return false, nil
	} else {
		println("There has been changes in the data, Merkle tree is not valid")
		return true, nil
	}

}
