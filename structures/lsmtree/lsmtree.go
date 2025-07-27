package lsmtree

import (
	"bytes"
	"container/heap"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/bloomfilter"
	"github.com/iigor000/database/structures/compression"
	"github.com/iigor000/database/structures/merkle"
	"github.com/iigor000/database/structures/sstable"
)

// Get traži vrednost za dati ključ u LSM stablu
// Vraća najnoviji DataRecord ukoliko je pronađen (na najnižem LSM nivou),
// Ako je isti key pronađen u više SSTable-ova, vraća vrednost sa najnovijim timestamp-om
func Get(conf *config.Config, key []byte, dict *compression.Dictionary) (*sstable.DataRecord, error) {
	maxLevel := conf.LSMTree.MaxLevel

	for level := 1; level < maxLevel; level++ {
		tables, err := getSSTablesByLevel(conf, level, dict)
		if err != nil {
			return nil, err
		}

		var record *sstable.DataRecord = nil

		for _, table := range tables {

			rec, _ := table.Get(conf, key)

			if record == nil {
				record = rec
			}
			if rec != nil && rec.Timestamp > record.Timestamp {
				record = rec
			}
		}

		if record != nil {
			return record, nil
		}
	}

	return nil, nil // Ako nije pronađen ključ ni u jednom nivou
}

// GetNextSSTableGeneration vraća sledeću generaciju SSTable-a na datom nivou
// Proverava da li postoji direktorijum za nivo i čita sve fajlove u direktorijumu da bi pronašao najveću generaciju
func GetNextSSTableGeneration(conf *config.Config, level int) int {
	// Proveri da li postoji direktorijum za nivo
	dir := fmt.Sprintf("%s/%d", conf.SSTable.SstableDirectory, level)
	if !sstable.FileExists(dir) {
		return 1 // Ako direktorijum ne postoji, prva generacija je 1
	}

	// Pročitaj sve fajlove u direktorijumu i pronađi najveću generaciju
	entries, err := os.ReadDir(dir)
	if err != nil {
		panic(fmt.Errorf("failed to read level %d directory '%s' : %w", level, dir, err))
	}

	maxGen := 1
	for _, entry := range entries {
		if entry.IsDir() {
			gen, err := strconv.Atoi(entry.Name())
			if err == nil && gen > maxGen {
				maxGen = gen
			}
		}
	}

	return maxGen + 1 // Vraća sledeću generaciju
}

// getSSTablesByLevel vraća sve SSTable-ove na datom nivou, sortirane po generaciji
func getSSTablesByLevel(conf *config.Config, level int, dict *compression.Dictionary) ([]*sstable.SSTable, error) {
	dir := fmt.Sprintf("%s/%d", conf.SSTable.SstableDirectory, level)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to read level %d directory '%s' : %w", level, dir, err)
	}

	var tables []*sstable.SSTable

	for _, entry := range entries {
		if entry.IsDir() {
			genDir := filepath.Join(dir, entry.Name())
			gen, err := strconv.Atoi(entry.Name())
			if err != nil {
				return nil, fmt.Errorf("failed to parse generation from directory name '%s': %w", genDir, err)
			}

			tocPath := filepath.Join(genDir, fmt.Sprintf("usertable-%06d-Data.db", gen))
			singlefile := filepath.Join(genDir, fmt.Sprintf("usertable-%06d-SSTable.db", gen))

			if sstable.FileExists(tocPath) || sstable.FileExists(singlefile) {
				table, err := sstable.StartSSTable(level, gen, conf, dict)
				if err != nil {
					return nil, fmt.Errorf("failed to start SSTable for level %d generation %d: %w", level, gen, err)
				}
				tables = append(tables, table)
			}

		}
	}

	sortSSTablesByGen(tables)
	return tables, err
}

// sortSSTablesByGen sortira SSTable-ove po generaciji
// Ako je reverse == true, sortira u opadajućem redosledu
func sortSSTablesByGen(tables []*sstable.SSTable, reverse ...bool) {
	inAscendingOrder := len(reverse) == 0 || reverse[0]

	if inAscendingOrder {
		// sortiraj u rastućem redosledu
		sort.Slice(tables, func(i, j int) bool {
			return tables[i].Gen < tables[j].Gen
		})
	} else {
		// sortiraj u opadajućem redosledu
		sort.Slice(tables, func(i, j int) bool {
			return tables[i].Gen > tables[j].Gen
		})
	}
}

// Compact pokreće kompakciju LSM stabla spajanjem SSTable-ova
// Kompakcija se vrši na osnovu podešavanja u konfiguraciji samo ako je ostvaren uslov za kompakciju
func Compact(conf *config.Config, dict *compression.Dictionary) error {
	if conf.LSMTree.CompactionAlgorithm == "size_tiered" {
		err := sizeTieredCompaction(conf, dict)
		if err != nil {
			return fmt.Errorf("error during size-tiered compaction: %w", err)
		}
	} else if conf.LSMTree.CompactionAlgorithm == "leveled" {
		err := leveledCompaction(conf, 1, dict)
		if err != nil {
			return fmt.Errorf("error during leveled compaction: %w", err)
		}
	} else {
		return fmt.Errorf("unknown compaction algorithm: %s", conf.LSMTree.CompactionAlgorithm)
	}
	return nil
}

// sizeTieredCompaction vrši kompakciju na osnovu broja SSTable-ova na nivou
func sizeTieredCompaction(conf *config.Config, dict *compression.Dictionary) error {
	maxLevel := conf.LSMTree.MaxLevel

	level := 1

	for level < maxLevel {
		maxSSTablesPerLevel := conf.LSMTree.MaxTablesPerLevel

		tables, err := getSSTablesByLevel(conf, level, dict)
		if err != nil {
			return fmt.Errorf("error getting SSTables for level %d: %v", level, err)
		}

		if len(tables) == 0 || len(tables) == 1 {
			return nil // Nema SSTable-ova na ovom nivou ili ima samo jedan, ništa ne radimo
		}

		for len(tables) >= maxSSTablesPerLevel {
			sst1 := tables[0]
			sst2 := tables[1]

			// Spaja prve dve tabele i kreira novu SSTable na sledećem nivou
			// briše stare SSTable-ove
			err := mergeTables(conf, level+1, sst1, sst2)
			if err != nil {
				return fmt.Errorf("error merging tables for level %d: %w", level, err)
			}
		}

		level++ // Pređi na sledeći nivo
	}

	return nil
}

// needCompaction proverava da li je potrebno izvršiti kompakciju na datom nivou (za Size-Tiered kompakciju)
func needCompaction(conf *config.Config, level int, tables []*sstable.SSTable) (bool, error) {
	if len(tables) == 0 {
		return false, nil
	}

	maxSSTablesSize := conf.LSMTree.BaseSSTableLimit * int(math.Pow(float64(conf.LSMTree.LevelSizeMultiplier), float64(level)))

	totalDataSize := 0
	// Proverava da li je data block size na nivou veći od maksimalnog
	for _, table := range tables {
		totalDataSize += int(table.Data.DataFile.SizeOnDisk)

		if totalDataSize > maxSSTablesSize {
			return true, nil // Ako je ukupna veličina podataka veća od maksimalne, potrebno je izvršiti kompakciju
		}
	}

	return false, nil
}

// getOverlappingSSTables vraća sve SSTable-ove na sledećem nivou koji se preklapaju sa datim SSTable-om
// Preklapanje se vrši na osnovu ključeva u Summary-ju
func getOverlappingSSTables(conf *config.Config, nextLevel int, minSSTKey []byte, maxSSTKey []byte, dict *compression.Dictionary) ([]*sstable.SSTable, error) {
	nextLevelTables, err := getSSTablesByLevel(conf, nextLevel, dict)
	if err != nil {
		return nil, fmt.Errorf("error getting SSTables for level %d: %v", nextLevel, err)
	}

	var overlapping []*sstable.SSTable

	for _, table := range nextLevelTables {
		minKey := table.Summary.FirstKey
		maxKey := table.Summary.LastKey

		if bytes.Compare(minKey, maxSSTKey) > 0 || bytes.Compare(maxKey, minSSTKey) < 0 {
			continue // Nema preklapanja, nastavi dalje
		}

		// Ako je preklapanje, dodaj tabelu u listu
		overlapping = append(overlapping, table)
	}

	return overlapping, nil
}

// leveledCompaction vrši kompakciju spram granice za nivo (maxSSTablesSize = BaseSSTableLimit * LevelSizeMultiplier^(Level))
// Vršimo kompakciju na ovom nivou sve dok ne dostigne Size ispod granice, pa tek onda na sledećem nivou
func leveledCompaction(conf *config.Config, level int, dict *compression.Dictionary) error {
	compactionDone := false

	// Vršimo kompakciju na ovom nivou sve dok ne dostigne određeni Size, pa tek onda na sledećem nivou
	for ; ; compactionDone = true {
		tables, err := getSSTablesByLevel(conf, level, dict)
		if err != nil {
			return fmt.Errorf("error getting SSTables for level %d: %w", level, err)
		}

		toCompact, err := needCompaction(conf, level, tables)
		if err != nil {
			return err
		}

		if !toCompact {
			break // Nema (više) potrebe za kompakcijom na ovom nivou
		}

		if len(tables) == 0 {
			return nil // Nema SSTable-ova na ovom nivou, ništa ne radimo
		}

		sst := tables[0] // Uzmi prvi SSTable za kompakciju

		overlapping, err := getOverlappingSSTables(conf, level+1, sst.Summary.FirstKey, sst.Summary.LastKey, dict)
		if err != nil {
			return fmt.Errorf("error getting overlapping SSTables for level %d: %w", level+1, err)
		}

		// Spaja prvi SSTable sa svim preklapajućim SSTable-ovima
		// briše stare SSTable-ove
		err = mergeTables(conf, level+1, sst, overlapping...)
		if err != nil {
			return fmt.Errorf("error merging tables for level %d: %w", level, err)
		}
	}

	if compactionDone {
		leveledCompaction(conf, level+1, dict)
	}

	return nil
}

// buildSSTableFromRecords pravi novu SSTable na osnovu spojenih zapisa
// Spaja sve zapise u jednu SSTable, pravi BloomFilter, Merkle stablo i ostale potrebne strukture
func buildSSTableFromRecords(mergedRecords []*sstable.DataRecord, conf *config.Config, newLevel int, newGen int) error {
	newSST := sstable.NewEmptySSTable(conf, newLevel, newGen)

	// Pripremi Data
	db := &sstable.Data{}
	for _, record := range mergedRecords {
		db.Records = append(db.Records, *record)
		if conf.SSTable.UseCompression {
			newSST.CompressionKey.Add(record.Key)
		}
	}
	newSST.Data = db

	// Pripremi Index
	ib := &sstable.Index{}
	for _, record := range mergedRecords {
		ir := sstable.NewIndexRecord(record.Key, record.Offset)
		ib.Records = append(ib.Records, ir)
	}
	newSST.Index = ib

	// Pripremi Summary
	summaryLevel := conf.SSTable.SummaryLevel
	sb := &sstable.Summary{}
	for i := 0; i < len(ib.Records); i += summaryLevel {
		// Pravimo summary sa onolko koliko je ostalo
		end := i + summaryLevel
		if end > len(ib.Records) {
			end = len(ib.Records)
		}

		sr := sstable.SummaryRecord{
			FirstKey:        ib.Records[i].Key,
			IndexOffset:     ib.Records[i].IndexOffset,
			NumberOfRecords: end - i,
		}
		sb.Records = append(sb.Records, sr)
	}
	newSST.Summary = sb

	// Pripremi BloomFilter
	fb := bloomfilter.MakeBloomFilter(len(mergedRecords), 0.5)
	for _, record := range mergedRecords {
		fb.Add(record.Key)
		if !record.Tombstone {
			fb.Add(record.Value)
		}
	}
	newSST.Filter = fb

	// Pripremi Merkle stablo
	data := make([][]byte, len(mergedRecords))
	for i, record := range mergedRecords {
		data[i] = record.Key
		if !record.Tombstone {
			data[i] = append(data[i], record.Value...)
		}
	}
	newSST.Metadata = merkle.NewMerkleTree(data)

	dir := fmt.Sprintf("%s/%d", conf.SSTable.SstableDirectory, newLevel)
	// Upiši SSTable u fajl
	sstable.WriteSSTable(newSST, dir, conf)

	return nil
}

// mergeRecords spaja zapise iz više SSTable-ova u jedan
// Vraća listu spojenih zapisa, sortiranu po ključu i timestamp-u
// Duplikati se uklanjaju, ostavljajući samo najnoviji zapis za svaki ključ
func mergeRecordsIter(sst1 *sstable.SSTable, ssts ...*sstable.SSTable) []*sstable.DataRecord {
	var minHeap MinHeap

	// Kreiramo sve iteratore
	allTables := append([]*sstable.SSTable{sst1}, ssts...)
	for _, table := range allTables {
		it := NewRecordIterator(table.Data.Records)
		if it.HasNext() {
			minHeap = append(minHeap, &HeapItem{record: it.Peek(), iterator: it})
		}
	}

	heap.Init(&minHeap)

	result := []*sstable.DataRecord{}
	var latestRec *sstable.DataRecord

	for len(minHeap) > 0 {
		// Ukloni najmanji element
		sort.Slice(minHeap, func(i, j int) bool {
			return bytes.Compare(minHeap[i].record.Key, minHeap[j].record.Key) < 0
		})

		current := minHeap[0]
		minHeap = minHeap[1:]

		if latestRec != nil && bytes.Equal(latestRec.Key, current.record.Key) {
			// isti key -> proveri timestamp
			if current.record.Timestamp > latestRec.Timestamp {
				latestRec = current.record
			}
		} else {
			// novi key
			if latestRec != nil && !latestRec.Tombstone {
				result = append(result, latestRec)
			}
			latestRec = current.record
		}

		// Popuni heap sa sledećim iz iteratora
		current.iterator.Next()
		if current.iterator.HasNext() {
			minHeap = append(minHeap, &HeapItem{
				record:   current.iterator.Peek(),
				iterator: current.iterator,
			})
		}
	}

	// Dodaj i poslednji zapamćen zapis ako nije tombstone
	if latestRec != nil && !latestRec.Tombstone {
		result = append(result, latestRec)
	}

	return result
}

// mergeTables spaja dva ili više SSTable-ova u jedan novi SSTable na sledećem nivou
// Ako je merge uspešan, briše stare SSTable fajlove
func mergeTables(conf *config.Config, newLevel int, sst1 *sstable.SSTable, ssts ...*sstable.SSTable) error {
	// Spaja sve zapise iz sst1 i ssts u jedan
	mergedRecords := mergeRecordsIter(sst1, ssts...)
	nextGen := GetNextSSTableGeneration(conf, newLevel)

	// Kreira novi SSTable na sledećem nivou
	err := buildSSTableFromRecords(mergedRecords, conf, newLevel, nextGen)
	if err != nil {
		return fmt.Errorf("error building SSTable from merged records: %w", err)
	} else {
		// Ako je merge uspešan, obriši stare SSTable fajlove
		err = sst1.DeleteFiles(conf)
		if err != nil {
			return fmt.Errorf("error deleting files for SSTable %d: %w", sst1.Gen, err)
		}
		for _, sst := range ssts {
			err = sst.DeleteFiles(conf)
			if err != nil {
				return fmt.Errorf("error deleting files for SSTable %d: %w", sst.Gen, err)
			}
		}
		return nil
	}
}

// mergeRecords spaja zapise iz više SSTable-ova u jedan
// Vraća listu spojenih zapisa, sortiranu po ključu i timestamp-u
// Duplikati se uklanjaju, ostavljajući samo najnoviji zapis za svaki ključ
// func mergeRecordsAndWriteData(conf *config.Config, newLevel int, newGen int, sst1 *sstable.SSTable, ssts ...*sstable.SSTable) ([]*sstable.DataRecord, error) {
// 	var minHeap MinHeap
// 	dict := compression.NewDictionary()
// 	bm := block_organization.NewBlockManager(conf)
// 	path := fmt.Sprintf("%s/%d/%d", conf.SSTable.SstableDirectory, newLevel, newGen) // Putanja za SSTable (direktorijum novog nivoa)
// 	dataFilename := sstable.CreateFileName(path, newGen, "Data", "db")

// 	// Kreiramo sve iteratore i pravimo dictionary
// 	allTables := append([]*sstable.SSTable{sst1}, ssts...)
// 	for _, table := range allTables {
// 		it := NewRecordIterator(table.Data.Records)
// 		if it.HasNext() {
// 			minHeap = append(minHeap, &HeapItem{record: it.Peek(), iterator: it})
// 		}
// 		for _, record := range table.Data.Records {
// 			dict.Add(record.Key)
// 		}
// 	}

// 	heap.Init(&minHeap)

// 	result := []*sstable.DataRecord{}
// 	var latestRec *sstable.DataRecord

// 	rec := 0
// 	bn := 0

// 	for len(minHeap) > 0 {
// 		// Ukloni najmanji element
// 		sort.Slice(minHeap, func(i, j int) bool {
// 			return bytes.Compare(minHeap[i].record.Key, minHeap[j].record.Key) < 0
// 		})

// 		current := minHeap[0]
// 		minHeap = minHeap[1:]

// 		if latestRec != nil && bytes.Equal(latestRec.Key, current.record.Key) {
// 			// isti key -> proveri timestamp
// 			if current.record.Timestamp > latestRec.Timestamp {
// 				latestRec = current.record
// 			}
// 		} else {
// 			// novi key
// 			if latestRec != nil && !latestRec.Tombstone {
// 				bn, err := latestRec.WriteDataRecord(dataFilename, dict, bm)
// 				if err != nil {
// 					return nil, fmt.Errorf("error writing data record to file %s: %w", dataFilename, err)
// 				}
// 				result = append(result, latestRec)
// 				result[rec].Offset = bn * conf.Block.BlockSize // Racunamo ofset kao broj bloka pomnozen sa velicinom bloka
// 				rec++
// 			}
// 			latestRec = current.record
// 		}

// 		// Popuni heap sa sledećim iz iteratora
// 		current.iterator.Next()
// 		if current.iterator.HasNext() {
// 			minHeap = append(minHeap, &HeapItem{
// 				record:   current.iterator.Peek(),
// 				iterator: current.iterator,
// 			})
// 		}
// 	}

// 	// Dodaj i poslednji zapamćen zapis ako nije tombstone
// 	if latestRec != nil && !latestRec.Tombstone {
// 		bn, err := latestRec.WriteDataRecord(dataFilename, dict, bm)
// 		if err != nil {
// 			return nil, fmt.Errorf("error writing data record to file %s: %w", dataFilename, err)
// 		}
// 		result[rec].Offset = bn * conf.Block.BlockSize // Racunamo ofset kao broj bloka pomnozen sa velicinom bloka
// 	}

// 	db.DataFile.SizeOnDisk = int64(bn * conf.Block.BlockSize)

// 	return result, nil
// }
