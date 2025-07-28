package lsmtree

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"strconv"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/block_organization"
	"github.com/iigor000/database/structures/compression"
	"github.com/iigor000/database/structures/sstable"
)

// Get traži vrednost za dati ključ u LSM stablu
// Vraća najnoviji DataRecord ukoliko je pronađen (na najnižem LSM nivou),
// Ako je isti key pronađen u više SSTable-ova, vraća vrednost sa najnovijim timestamp-om
func Get(conf *config.Config, key []byte, dict *compression.Dictionary, cbm *block_organization.CachedBlockManager) (*sstable.DataRecord, error) {
	maxLevel := conf.LSMTree.MaxLevel

	for level := 1; level < maxLevel; level++ {
		refs, err := getSSTableReferences(conf, level, false) // Sortiraj po generaciji u opadajućem redosledu (najnoviji podaci su kod većih generacija)
		if err != nil {
			return nil, err
		}
		var record *sstable.DataRecord = nil
		for _, ref := range refs {
			fmt.Print("Otvaram SSTable za nivo ", ref.Level, ", generacija ", ref.Gen, "...\n")
			table, err := sstable.StartSSTable(ref.Level, ref.Gen, conf, dict, cbm)
			if err != nil {
				return nil, fmt.Errorf("failed to open SSTable for level %d, gen %d: %w", ref.Level, ref.Gen, err)
			}

			fmt.Print("Tražim ključ ", string(key), " u SSTable...\n")
			rec, _ := table.Get(conf, key, cbm)

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

// Compact pokreće kompakciju LSM stabla spajanjem SSTable-ova
// Kompakcija se vrši na osnovu podešavanja u konfiguraciji samo ako je ostvaren uslov za kompakciju
func Compact(conf *config.Config, dict *compression.Dictionary, cbm *block_organization.CachedBlockManager) error {
	if conf.LSMTree.CompactionAlgorithm == "size_tiered" {
		err := sizeTieredCompaction(conf, dict, cbm)
		if err != nil {
			return fmt.Errorf("error during size-tiered compaction: %w", err)
		}
	} else if conf.LSMTree.CompactionAlgorithm == "leveled" {
		err := leveledCompaction(conf, 1, dict, cbm)
		if err != nil {
			return fmt.Errorf("error during leveled compaction: %w", err)
		}
	} else {
		return fmt.Errorf("unknown compaction algorithm: %s", conf.LSMTree.CompactionAlgorithm)
	}
	return nil
}

// sizeTieredCompaction vrši kompakciju na osnovu broja SSTable-ova na nivou
func sizeTieredCompaction(conf *config.Config, dict *compression.Dictionary, cbm *block_organization.CachedBlockManager) error {
	maxLevel := conf.LSMTree.MaxLevel

	level := 1

	for level < maxLevel {
		maxSSTablesPerLevel := conf.LSMTree.MaxTablesPerLevel

		refs, err := getSSTableReferences(conf, level, true) // Sortiraj po generaciji u rastućem redosledu (želimo da kompaktujemo najstarije)
		if err != nil {
			return fmt.Errorf("error getting SSTable references for level %d: %v", level, err)
		}

		if len(refs) == 0 || len(refs) == 1 {
			return nil // Nema SSTable-ova na ovom nivou ili ima samo jedan, ništa ne radimo
		}

		for len(refs) >= maxSSTablesPerLevel {
			// Spaja prve dve tabele i kreira novu SSTable na sledećem nivou
			// briše stare SSTable-ove
			err = mergeTables(conf, level+1, cbm, dict, refs[0], refs[1])
			if err != nil {
				return fmt.Errorf("error merging tables for level %d: %w", level, err)
			}
		}

		level++ // Pređi na sledeći nivo
	}

	return nil
}

// needCompaction proverava da li je potrebno izvršiti kompakciju na datom nivou (za Leveled kompakciju)
func needCompaction(conf *config.Config, level int, refs []*SSTableReference) (bool, error) {
	if len(refs) == 0 {
		return false, nil
	}

	maxSSTablesSize := conf.LSMTree.BaseSSTableLimit * int(math.Pow(float64(conf.LSMTree.LevelSizeMultiplier), float64(level)))

	totalDataSize := 0
	// Proverava da li je data block size na nivou veći od maksimalnog
	for _, ref := range refs {
		path := fmt.Sprintf("%s/%d/%d", conf.SSTable.SstableDirectory, ref.Level, ref.Gen)
		if conf.SSTable.SingleFile {
			path = sstable.CreateFileName(fmt.Sprintf("%s/%d", conf.SSTable.SstableDirectory, ref.Level), ref.Gen, "SSTable", "db")
		}
		size := sstable.CalculateDataSize(path, conf)

		totalDataSize += int(size)

		if totalDataSize > maxSSTablesSize {
			return true, nil // Ako je ukupna veličina podataka veća od maksimalne, potrebno je izvršiti kompakciju
		}
	}

	return false, nil
}

// getOverlappingReferences vraća sve reference na SSTable-ove na sledećem nivou koji se preklapaju sa datim SSTable-om
// Preklapanje se vrši na osnovu ključeva u Summary-ju
func getOverlappingReferences(conf *config.Config, nextLevel int, minSSTKey []byte, maxSSTKey []byte, dict *compression.Dictionary, cbm *block_organization.CachedBlockManager) ([]*SSTableReference, error) {
	refs, err := getSSTableReferences(conf, nextLevel, true)
	if err != nil {
		return nil, fmt.Errorf("error getting SSTable references for level %d: %v", nextLevel, err)
	}

	var overlapping []*SSTableReference

	for _, ref := range refs {
		minKey, maxKey, err := sstable.ReadSummaryMinMax(ref.Level, ref.Gen, conf, cbm)
		if err != nil {
			return nil, fmt.Errorf("failed to read summary min/max for level %d, gen %d: %w", ref.Level, ref.Gen, err)
		}

		if bytes.Compare(minKey, maxSSTKey) > 0 || bytes.Compare(maxKey, minSSTKey) < 0 {
			continue // Nema preklapanja, nastavi dalje
		}

		// Ako je preklapanje, dodaj referencu u listu
		overlapping = append(overlapping, ref)
	}

	return overlapping, nil
}

// leveledCompaction vrši kompakciju spram granice za nivo (maxSSTablesSize = BaseSSTableLimit * LevelSizeMultiplier^(Level))
// Vršimo kompakciju na ovom nivou sve dok ne dostigne Size ispod granice, pa tek onda na sledećem nivou
func leveledCompaction(conf *config.Config, level int, dict *compression.Dictionary, cbm *block_organization.CachedBlockManager) error {
	compactionDone := false

	// Vršimo kompakciju na ovom nivou sve dok ne dostigne određeni Size, pa tek onda na sledećem nivou
	for ; ; compactionDone = true {
		refs, err := getSSTableReferences(conf, level, true) // Sortiraj po generaciji u rastućem redosledu (želimo da kompaktujemo najstarije)
		if err != nil {
			return fmt.Errorf("error getting SSTable references for level %d: %w", level, err)
		}

		toCompact, err := needCompaction(conf, level, refs)
		if err != nil {
			return err
		}

		if !toCompact {
			break // Nema (više) potrebe za kompakcijom na ovom nivou
		}

		if len(refs) == 0 {
			return nil // Nema SSTable-ova na ovom nivou, ništa ne radimo
		}

		// Poredimo prvu SSTable sa svim ostalim na sledećem nivou
		minKey, maxKey, err := sstable.ReadSummaryMinMax(refs[0].Level, refs[0].Gen, conf, cbm)
		if err != nil {
			return fmt.Errorf("failed to read summary min/max for level %d, gen %d: %w", refs[0].Level, refs[0].Gen, err)
		}

		overlapping, err := getOverlappingReferences(conf, level+1, minKey, maxKey, dict, cbm)
		if err != nil {
			return fmt.Errorf("error getting overlapping SSTables for level %d: %w", level+1, err)
		}
		// Spaja prvi SSTable sa svim preklapajućim SSTable-ovima
		// briše stare SSTable-ove
		err = mergeTables(conf, level+1, cbm, dict, refs[0], overlapping...)
		if err != nil {
			return fmt.Errorf("error merging tables for level %d: %w", level, err)
		}
	}

	if compactionDone {
		leveledCompaction(conf, level+1, dict, cbm)
	}

	return nil
}

func cleanupNames(conf *config.Config, level int) error {
	references, err := getSSTableReferences(conf, level, true) // Sortiraj po generaciji u rastućem redosledu
	if err != nil {
		return fmt.Errorf("failed to get SSTable references for level %d: %w", level, err)
	}

	for i, ref := range references {
		genTarget := i + 1

		if ref.Gen == genTarget {
			continue
		}

		// Preimenuj fajlove na starom nivou
		err := Rename(conf, ref.Level, ref.Gen, genTarget)
		if err != nil {
			return fmt.Errorf("failed to rename SSTable files for level %d, gen %d: %w", ref.Level, ref.Gen, err)
		}
	}
	return nil
}

// mergeTables spaja dva ili više SSTable-ova u jedan novi SSTable i upisuje ga na newLevel
// Briše stare fajlove SSTable-ova koji su spojeni
func mergeTables(conf *config.Config, newLevel int, cbm *block_organization.CachedBlockManager, dict *compression.Dictionary, sst1 *SSTableReference, ssts ...*SSTableReference) error {
	allRefs := append([]*SSTableReference{sst1}, ssts...)
	tables := make([]*sstable.SSTable, 0, len(allRefs))

	for _, ref := range allRefs {
		table, err := sstable.StartSSTable(ref.Level, ref.Gen, conf, dict, cbm)
		if err != nil {
			return fmt.Errorf("failed to start SSTable for level %d, gen %d:%w", ref.Level, ref.Gen, err)
		}
		tables = append(tables, table)
	}

	iter := NewLSMTreeIterator(tables, cbm)

	// Kreiraj novi SSTable builder
	nextGen := GetNextSSTableGeneration(conf, newLevel)
	builder, err := NewSSTableBuilder(newLevel, nextGen, conf)
	if err != nil {
		return fmt.Errorf("failed to create new SSTable builder: %w", err)
	}

	fmt.Print("Spajam SSTable-ove na nivou ", newLevel, "...\n")

	for {
		if iter == nil {
			fmt.Print("Iter je nil...\n")
			break // Nema više SSTable-ova za spajanje
		}

		entry := iter.Next()

		if entry == nil {
			break // Nema više zapisa za spajanje
		}

		if entry.Key == nil {
			break // Nema više elemenata za iteraciju
		}

		fmt.Print("Zapis sa ključem ", string(entry.Key), " i vrednošću ", string(entry.Value), " je pronađen...\n")

		if entry.Tombstone {
			continue // preskoči obrisane
		}

		err := builder.Write(*entry)
		if err != nil {
			return fmt.Errorf("failed to write entry: %w", err)
		}
	}

	fmt.Print("Završavam spajanje SSTable-ova na nivou ", newLevel, "...\n")

	err = builder.Finish(cbm, dict)
	if err != nil {
		return fmt.Errorf("failed to finish SSTable build: %w", err)
	}

	// Obriši stare SSTable-ove (HANDLE: ovo može biti opasno zbog drugačijeg imenovanja)
	if err := sst1.DeleteFiles(conf); err != nil {
		return err
	}
	for _, s := range ssts {
		if err := s.DeleteFiles(conf); err != nil {
			return err
		}
	}

	// Preimenuj fajlove na nivoima
	cleanupNames(conf, newLevel-1)
	cleanupNames(conf, newLevel)

	return nil
}
