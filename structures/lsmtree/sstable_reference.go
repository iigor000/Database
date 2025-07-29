package lsmtree

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/sstable"
)

// SSTableReference predstavlja referencu na SSTable na određenom nivou i generaciji
// Koristi se za identifikaciju i upravljanje SSTable-ovima u sistemu
type SSTableReference struct {
	Level int // Nivo SSTable-a
	Gen   int // Generacija SSTable-a
}

// getSSTableReferences vraća sve SSTable-ove na datom nivou, sortirane po generaciji
func getSSTableReferences(conf *config.Config, level int, ascending bool) ([]*SSTableReference, error) {
	dir := fmt.Sprintf("%s/%d", conf.SSTable.SstableDirectory, level)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to read level %d directory '%s' : %w", level, dir, err)
	}
	var refs []*SSTableReference

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
				refs = append(refs, &SSTableReference{Level: level, Gen: gen})
			}

		}
	}

	sortReferencesByGen(refs, ascending) // Sortiraj po generaciji
	return refs, err
}

// sortReferencesByGen sortira SSTableReference-ove po generaciji
// Ako je ascending == true, sortira u rastućem redosledu
func sortReferencesByGen(refs []*SSTableReference, ascending ...bool) {
	inAscendingOrder := len(ascending) == 0 || ascending[0]

	if inAscendingOrder {
		// sortiraj u rastućem redosledu
		sort.Slice(refs, func(i, j int) bool {
			return refs[i].Gen < refs[j].Gen
		})
	} else {
		// sortiraj u opadajućem redosledu
		sort.Slice(refs, func(i, j int) bool {
			return refs[i].Gen > refs[j].Gen
		})
	}
}

// DeleteFiles briše sve fajlove vezane za odgovarajući SSTable
func (s *SSTableReference) DeleteFiles(conf *config.Config) error {
	sstableDir := fmt.Sprintf("%s/%d/%d", conf.SSTable.SstableDirectory, s.Level, s.Gen)

	err := os.RemoveAll(sstableDir)
	if err != nil {
		return fmt.Errorf("failed to remove SSTable directory %s: %w", sstableDir, err)
	}

	return nil
}