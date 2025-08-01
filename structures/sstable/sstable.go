package sstable

import (
	"bytes"
	"fmt"
	"os"
	"sort"
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

// FlushSSTable kreira SSTable iz Memtable i upisuje je na disk
func FlushSSTable(conf *config.Config, memtable memtable.Memtable, level int, generation int, dict *compression.Dictionary, cbm *block_organization.CachedBlockManager) *SSTable {
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

	sstable.Gen = generation
	path := fmt.Sprintf("%s/%d/%d", conf.SSTable.SstableDirectory, level, sstable.Gen)
	err := CreateDirectoryIfNotExists(path)
	if err != nil {
		panic("Error creating directory for SSTable: " + err.Error())
	}

	sstable.SingleFile = conf.SSTable.SingleFile
	if sstable.SingleFile {
		fm := CreateFileName(path, generation, "SSTable", "db")
		_, err := cbm.AppendBlock(fm, []byte("TOC"))
		if err != nil {
			panic("Error creating single file SSTable: " + err.Error())
		}
	}
	sstable.Data = buildData(memtable, conf, generation, path, sstable.SingleFile, dict, cbm)
	sstable.Index = buildIndex(conf, generation, path, sstable.Data, sstable.SingleFile, cbm)
	sstable.Summary = buildSummary(conf, sstable.Index, generation, path, sstable.SingleFile, cbm)
	sstable.Filter = buildBloomFilter(conf, generation, path, sstable.Data, sstable.SingleFile, cbm)
	sstable.Metadata = buildMetadata(generation, path, sstable.Data, sstable.SingleFile, conf, cbm)
	if !sstable.SingleFile {
		dictPath := CreateFileName(path, generation, "CompressionInfo", "db")
		// Upisujemo true ili false u fajl da li koristimo kompresiju
		sstable.WriteCompressionInfo(dictPath, dict, conf, cbm)
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
		if err := sstable.WriteSingleFile(path, conf, cbm); err != nil {
			fmt.Printf("Error writing single file SSTable: %v\n", err)
		}
	}

	sstable.Dir = path
	return &sstable
}

func (sstable *SSTable) WriteSingleFile(path string, conf *config.Config, cbm *block_organization.CachedBlockManager) error {
	sstable.Data.DataFile.Offset = int64(1 * conf.Block.BlockSize)
	sstable.Data.DataFile.SizeOnDisk = sstable.Index.IndexFile.Offset - sstable.Data.DataFile.Offset
	sstable.Index.IndexFile.SizeOnDisk = sstable.Summary.SummaryFile.Offset - sstable.Index.IndexFile.Offset
	serializedFilter := sstable.Filter.Serialize()
	bn, err := cbm.Append(path, serializedFilter)
	if err != nil {
		return fmt.Errorf("error writing filter to file: %w", err)
	}
	filterOffset := int64(bn * cbm.BM.BlockSize)

	metadata, err := sstable.Metadata.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing metadata: %w", err)
	}
	bn, err = cbm.Append(path, metadata)
	if err != nil {
		return fmt.Errorf("error writing metadata to file: %w", err)
	}
	metadataOffset := int64(bn * cbm.BM.BlockSize)

	var compressionByte []byte
	if sstable.UseCompression {
		compressionByte = []byte("Using compression")
	} else {
		compressionByte = []byte("No compression")
	}
	bn, err = cbm.Append(path, compressionByte)
	if err != nil {
		return fmt.Errorf("error writing compression info: %w", err)
	}
	compressionOffset := int64(bn * cbm.BM.BlockSize)

	// Upisujemo TOC u fajl
	offsets := make(map[string]int64)
	offsets["Data"] = sstable.Data.DataFile.Offset
	offsets["Index"] = sstable.Index.IndexFile.Offset
	offsets["Summary"] = sstable.Summary.SummaryFile.Offset
	offsets["Filter"] = filterOffset
	offsets["Metadata"] = metadataOffset
	offsets["Compression"] = compressionOffset
	_, err = cbm.Append(path, []byte("TOC\n"))
	if err != nil {
		return fmt.Errorf("error writing TOC header: %w", err)
	}
	serializedToc := []byte{}
	for key, offset := range offsets {
		//println("Writing TOC entry:", key, "at offset", offset)
		serializedToc = append(serializedToc, []byte(fmt.Sprintf("%s: %d\n", key, offset))...)
	}
	err = cbm.WriteBlock(path, 0, serializedToc)
	if err != nil {
		return fmt.Errorf("error writing TOC entries: %w", err)
	}
	return nil
}

func (s *SSTable) WriteCompressionInfo(path string, dict *compression.Dictionary, conf *config.Config, bm *block_organization.CachedBlockManager) {
	data := []byte{0}
	if s.UseCompression && dict != nil && !dict.IsEmpty() {
		data[0] = 1
	} else {
		data[0] = 0
	}
	_, err := bm.Append(path, data)
	if err != nil {
		panic("Error writing compression info to file: " + err.Error())
	}
}

func ReadCompressionInfo(path string, conf *config.Config, bm *block_organization.CachedBlockManager) (bool, error) {
	block, err := bm.Read(path, 0)
	if err != nil {
		return false, fmt.Errorf("error reading compression info from file %s: %w", path, err)
	}
	return block[0] == 1, nil
}

func buildData(mem memtable.Memtable, conf *config.Config, gen int, path string, singleFile bool, dict *compression.Dictionary, cbm *block_organization.CachedBlockManager) *Data {
	db := &Data{}
	// Use len(mem.Keys) instead of mem.Capacity to avoid index out of bounds
	for i := 0; i < len(mem.Keys); i++ {
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
		db.WriteData(filename, conf, dict, cbm)
	} else {
		db.WriteData(filename, conf, nil, cbm)
	}
	return db
}

func buildIndex(conf *config.Config, gen int, path string, db *Data, singleFile bool, cbm *block_organization.CachedBlockManager) *Index {
	ib := &Index{}
	for _, record := range db.Records {
		ir := NewIndexRecord(record.Key, record.Offset)
		ib.Records = append(ib.Records, ir)
	}

	filename := CreateFileName(path, gen, "SSTable", "db")
	if !singleFile {
		filename = CreateFileName(path, gen, "Index", "db")
	}
	err := ib.WriteIndex(filename, conf, cbm)
	if err != nil {
		panic("Error writing index to file: " + err.Error())
	}
	return ib
}

func buildSummary(conf *config.Config, index *Index, gen int, path string, singleFile bool, cbm *block_organization.CachedBlockManager) *Summary {
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
	err := sb.WriteSummary(filename, conf, cbm)
	if err != nil {
		panic("Error writing summary to file: " + err.Error())
	}
	return sb
}

func buildBloomFilter(conf *config.Config, gen int, path string, db *Data, singleFile bool, cbm *block_organization.CachedBlockManager) bloomfilter.BloomFilter {
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
		// data sadrzi velicinu bloom filtera i sam bloom filter

		_, err := cbm.Append(filename, serialized)
		if err != nil {
			panic("Error writing bloom filter to file: " + err.Error())
		}
	}
	return fb
}

// buildMetadata kreira Merkle stablo i upisuje ga u fajl
func buildMetadata(gen int, path string, db *Data, singleFile bool, conf *config.Config, cbm *block_organization.CachedBlockManager) *merkle.MerkleTree {
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
		_, err := cbm.Append(filename, serialized)
		if err != nil {
			panic("Error writing bloom filter to file: " + err.Error())
		}
	}

	return mt
}

func ReadBloomFilter(path string, conf *config.Config, bm *block_organization.CachedBlockManager) (bloomfilter.BloomFilter, error) {
	block, err := bm.Read(path, 0)

	if err != nil {
		return bloomfilter.BloomFilter{}, fmt.Errorf("error reading bloom filter from file %s: %w", path, err)
	}
	fb := bloomfilter.Deserialize(block)
	return fb[0], nil
}

func ReadMetadata(path string, conf *config.Config, bm *block_organization.CachedBlockManager) (*merkle.MerkleTree, error) {
	m := &merkle.MerkleTree{}
	block, err := bm.Read(path, 0)
	if err != nil {
		return nil, fmt.Errorf("error reading Merkle tree from file %s: %w", path, err)
	}
	m, err = merkle.Deserialize(block)
	if err != nil {
		return nil, fmt.Errorf("error deserializing Merkle tree: %w", err)
	}
	return m, nil
}

func (sstable *SSTable) ReadFilterMetaCompression(path string, offsets map[string]int64, readMerkle bool, conf *config.Config, bm *block_organization.CachedBlockManager) error {
	//Citanje Compression info
	block, err := bm.Read(path, int(offsets["Compression"]/int64(conf.Block.BlockSize)))
	if err != nil {
		return fmt.Errorf("error reading compression info from file: %w", err)
	}
	str_block := strings.TrimRight(string(block), "\x00")
	UsingCompression := str_block == "Using compression"
	sstable.UseCompression = UsingCompression
	// Citanje Bloom filtera
	block, err = bm.Read(path, int(offsets["Filter"]/int64(conf.Block.BlockSize)))
	if err != nil {
		return fmt.Errorf("error reading Bloom filter from file: %w", err)
	}
	sstable.Filter = bloomfilter.Deserialize(block)[0]
	if readMerkle {
		// Citanje Merkle stabla
		block, err = bm.Read(path, int(offsets["Metadata"]/int64(conf.Block.BlockSize)))
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

// Kreira Stable od liste Data Record-a
func BuildSSTable(entries []adapter.MemtableEntry, conf *config.Config, dict *compression.Dictionary, cbm *block_organization.CachedBlockManager, level int, gen int) *SSTable {
	conf1 := &config.Config{
		Memtable: config.MemtableConfig{
			NumberOfMemtables: 1,
			NumberOfEntries:   len(entries),
			Structure:         "skiplist",
		},
	}

	memtable := memtable.NewMemtable(conf1)
	for _, entry := range entries {
		memtable.Update(entry.Key, entry.Value, entry.Timestamp, entry.Tombstone)
	}
	memtable.Capacity = memtable.Size
	return FlushSSTable(conf, *memtable, level, gen, dict, cbm)
}

// Get traži ključ u SSTable-u i vraća odgovarajući DataRecord
// Pomocna funkcija za LSM
func (s *SSTable) Get(conf *config.Config, key []byte, bm *block_organization.CachedBlockManager) (*DataRecord, error) {
	//println("SSTable Get called for key:", string(key))
	// Proveri Bloom filter pre pretrage
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	if !s.Filter.Read(keyCopy) {
		return nil, nil
	}

	// Ako Bloom filter sadrži ključ, proveri u summary i index
	if bytes.Compare(key, s.Summary.FirstKey) < 0 || bytes.Compare(key, s.Summary.LastKey) > 0 {

		return nil, nil // Ključ nije u ovom summary bloku
	}

	// Ako je ključ unutar opsega summary, proveri index
	// indexOffset je offset u Index segmentu gde se nalazi ovaj summary
	prefixIter := s.PrefixIterate(string(key), bm)
	if prefixIter == nil {
		return nil, nil // Nema zapisa sa tim prefiksom
	}
	rec, _ := prefixIter.Next()
	if !bytes.Equal(rec.Key, key) {
		return nil, nil
	}
	dr := DataRecord{
		Key:       rec.Key,
		Value:     rec.Value,
		Timestamp: rec.Timestamp,
		Tombstone: rec.Tombstone,
	}
	return &dr, nil
}

// Pomocna funkcija za PreffixIterate i RangeIterate
// Trazi prvi sledeci zapis koji pocinje sa prefiksom/key-em
// Prvo trazi u Summary, onda u Indexu, a zatim u Data segmentu
func (s *SSTable) ReadRecordWithKey(bm *block_organization.CachedBlockManager, blockNumber int, prefix string, rangeIter bool) (adapter.MemtableEntry, int) {

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
			return adapter.MemtableEntry{}, -1
		}
	}
	dataRec, nextBlock := s.Data.ReadRecord(bm, dataOffset/bm.BM.BlockSize, s.CompressionKey) // Citanje iz Data fajla
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

func ReadTOC(path string) map[string]string {
	toc := make(map[string]string)
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("failed to read TOC file '%s': %v\n", path, err)
		return toc
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		parts := strings.SplitN(line, ": ", 2)
		if len(parts) == 2 {
			toc[parts[0]] = parts[1]
		}
	}
	return toc
}

func StartSSTable(level int, gen int, conf *config.Config, dict *compression.Dictionary, cbm *block_organization.CachedBlockManager) (*SSTable, error) {
	// Ucitavamo bloom filter i summary iz fajla
	if gen < 1 {
		return nil, fmt.Errorf("invalid generation number: %d", gen)
	}
	dir := fmt.Sprintf("%s/%d/%d", conf.SSTable.SstableDirectory, level, gen)
	//Probaj da nadjes SSTable u jednom fajlu
	singleFile := false
	pathSingle := CreateFileName(dir, gen, "SSTable", "db")
	if _, err := os.Stat(pathSingle); err == nil {
		singleFile = true
	}

	if singleFile {
		sstable := &SSTable{
			Gen:        gen,
			Level:      level,
			SingleFile: singleFile,
			Dir:        dir,
		}
		path := CreateFileName(dir, gen, "SSTable", "db")
		offsets, err := ReadOffsetsFromFile(path, conf, cbm)
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
		err = sstable.ReadFilterMetaCompression(path, offsets, false, conf, cbm)
		if err != nil {
			panic("Error reading filter, metadata and compression info: " + err.Error())
		}
		if sstable.UseCompression {
			sstable.CompressionKey = dict

		} else {
			sstable.CompressionKey = nil
		}

		//Ucitavamo Summary
		summary, err := ReadSummary(sstable.Summary.SummaryFile.Path, conf, sstable.Summary.SummaryFile.Offset, sstable.FilterOffset, cbm)
		if err != nil {
			return nil, fmt.Errorf("error reading summary: %w", err)
		}
		sstable.Summary = summary
		return sstable, nil
	}
	//Ucitavamo TOC iz fajla
	tocPath := CreateFileName(dir, gen, "TOC", "txt")
	toc := ReadTOC(tocPath)

	// Ucitavamo BloomFiler
	bfPath := toc["Filter"]
	bf, err := ReadBloomFilter(bfPath, conf, cbm)
	if err != nil {
		return nil, fmt.Errorf("error reading bloom filter: %w", err)
	}
	// Ucitavamo Summary
	summaryPath := toc["Summary"]
	summary, err := ReadSummary(summaryPath, conf, 0, 0, cbm)
	if err != nil {
		return nil, fmt.Errorf("error reading summary: %w", err)
	}

	// Ucitavamo kompresiju
	dictpath := toc["Compression"]
	UseCompression, err := ReadCompressionInfo(dictpath, conf, cbm)
	if err != nil {
		return nil, fmt.Errorf("error reading compression dictionary: %w", err)
	}
	dictionary := dict
	if !UseCompression {
		dictionary = nil // Ako ne koristimo kompresiju, dictionary je nil
	}

	data := &Data{
		DataFile: File{
			Path:       toc["Data"],
			Offset:     0,
			SizeOnDisk: -1,
		},
		Records: []DataRecord{},
	}
	index := &Index{
		IndexFile: File{
			Path:       toc["Index"],
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
		SingleFile:     singleFile,
	}
	return sstable, nil
}

// ValidateMerkleTree proverava da li je doslo do izmene u podacima
// Ako jeste, vraca true, ako nije, vraca false
// Ako je doslo do greske u citanju podataka ili Merkle stabla, vraca gresku
func (sstable *SSTable) ValidateMerkleTree(conf *config.Config, dict *compression.Dictionary, bm *block_organization.CachedBlockManager) (bool, error) {

	filename := CreateFileName(sstable.Dir, sstable.Gen, "Data", "db")
	if sstable.SingleFile {
		filename = CreateFileName(sstable.Dir, sstable.Gen, "SSTable", "db")
	}
	if !sstable.UseCompression {
		dict = nil
	}
	db, err := ReadData(filename, conf, dict, sstable.Data.DataFile.Offset, sstable.Index.IndexFile.Offset, bm)
	if err != nil {
		return false, fmt.Errorf("error reading data: %w", err)
	}
	data := make([][]byte, len(db.Records))
	for i, record := range db.Records {
		data[i] = make([]byte, len(record.Key))
		copy(data[i], record.Key)
		if !record.Tombstone {
			data[i] = append(data[i], record.Value...)
		}
	}
	new_mt := merkle.NewMerkleTree(data)
	if !sstable.SingleFile {
		filename = CreateFileName(sstable.Dir, sstable.Gen, "Metadata", "db")
	}
	bn := 0
	if sstable.SingleFile {
		bn = int(sstable.MetadataOffset / int64(bm.BM.BlockSize))
	}
	block, err := bm.Read(filename, bn)
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
		println("Changes are on indexes:")
		println(old_mt.Compare(new_mt))
		return true, nil
	}

}
