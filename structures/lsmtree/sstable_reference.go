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
	println("Tražim SSTable-ove u direktorijumu:", dir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to read level %d directory '%s' : %w", level, dir, err)
	}
	println("Pronađen direktorijum:", dir)
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

	// Briše direktorijum i sve fajlove unutar njega
	if err := os.RemoveAll(sstableDir); err != nil {
		return fmt.Errorf("failed to remove SSTable directory %s: %w", sstableDir, err)
	}
	return nil
}

func Rename(conf *config.Config, level int, oldGen int, newGen int) error {
	oldDir := fmt.Sprintf("%s/%d/%d", conf.SSTable.SstableDirectory, level, oldGen)
	if !sstable.FileExists(oldDir) {
		return fmt.Errorf("old SSTable path does not exist: %s", oldDir)
	}

	if conf.SSTable.SingleFile {
		oldPath := sstable.CreateFileName(oldDir, oldGen, "SSTable", "db")
		if !sstable.FileExists(oldPath) {
			return fmt.Errorf("old SSTable file does not exist: %s", oldPath)
		}
		newPath := sstable.CreateFileName(oldDir, newGen, "SSTable", "db")

		changeTOC(oldDir, oldGen, newGen)

		if err := os.Rename(oldPath, newPath); err != nil {
			return fmt.Errorf("failed to rename SSTable file from %s to %s: %w", oldPath, newPath, err)
		}
	} else {
		oldBFPath := sstable.CreateFileName(oldDir, oldGen, "Filter", "db")
		oldSummaryPath := sstable.CreateFileName(oldDir, oldGen, "Summary", "db")
		oldDictPath := sstable.CreateFileName(oldDir, oldGen, "CompressionInfo", "db")
		oldDataPath := sstable.CreateFileName(oldDir, oldGen, "Data", "db")
		oldIndexPath := sstable.CreateFileName(oldDir, oldGen, "Index", "db")
		oldMetadataPath := sstable.CreateFileName(oldDir, oldGen, "Metadata", "db")
		oldTOCPath := sstable.CreateFileName(oldDir, oldGen, "TOC", "txt")
		if !sstable.FileExists(oldBFPath) || !sstable.FileExists(oldSummaryPath) ||
			!sstable.FileExists(oldDictPath) || !sstable.FileExists(oldDataPath) ||
			!sstable.FileExists(oldIndexPath) || !sstable.FileExists(oldMetadataPath) ||
			!sstable.FileExists(oldTOCPath) {
			return fmt.Errorf("old SSTable files do not exist in directory: %s", oldDir)
		}

		newBFPath := sstable.CreateFileName(oldDir, newGen, "Filter", "db")
		if err := os.Rename(oldBFPath, newBFPath); err != nil {
			return fmt.Errorf("failed to rename Bloom Filter file from %s to %s: %w", oldBFPath, newBFPath, err)
		}

		newSummaryPath := sstable.CreateFileName(oldDir, newGen, "Summary", "db")
		if err := os.Rename(oldSummaryPath, newSummaryPath); err != nil {
			return fmt.Errorf("failed to rename Summary file from %s to %s: %w", oldSummaryPath, newSummaryPath, err)
		}

		newDictPath := sstable.CreateFileName(oldDir, newGen, "CompressionInfo", "db")
		if err := os.Rename(oldDictPath, newDictPath); err != nil {
			return fmt.Errorf("failed to rename Compression Info file from %s to %s: %w", oldDictPath, newDictPath, err)
		}

		newDataPath := sstable.CreateFileName(oldDir, newGen, "Data", "db")
		if err := os.Rename(oldDataPath, newDataPath); err != nil {
			return fmt.Errorf("failed to rename Data file from %s to %s: %w", oldDataPath, newDataPath, err)
		}

		newIndexPath := sstable.CreateFileName(oldDir, newGen, "Index", "db")
		if err := os.Rename(oldIndexPath, newIndexPath); err != nil {
			return fmt.Errorf("failed to rename Index file from %s to %s: %w", oldIndexPath, newIndexPath, err)
		}

		newMetadataPath := sstable.CreateFileName(oldDir, newGen, "Metadata", "db")
		if err := os.Rename(oldMetadataPath, newMetadataPath); err != nil {
			return fmt.Errorf("failed to rename Metadata file from %s to %s: %w", oldMetadataPath, newMetadataPath, err)
		}

		changeTOC(oldDir, oldGen, newGen)

		newTOCPath := sstable.CreateFileName(oldDir, newGen, "TOC", "txt")
		if err := os.Rename(oldTOCPath, newTOCPath); err != nil {
			return fmt.Errorf("failed to rename TOC file from %s to %s: %w", oldTOCPath, newTOCPath, err)
		}
	}

	newDir := fmt.Sprintf("%s/%d/%d", conf.SSTable.SstableDirectory, level, newGen)

	if err := os.Rename(oldDir, newDir); err != nil {
		return fmt.Errorf("failed to rename SSTable directory from %s to %s: %w", oldDir, newDir, err)
	}

	return nil
}

func changeTOC(path string, oldGen, newGen int) {
	toc_data := fmt.Sprintf("Generation: %d\nData: %s\nIndex: %s\nSummary: %s\nFilter: %s\nMetadata: %s\nCompression: %s\n",
		newGen, sstable.CreateFileName(path, newGen, "Data", "db"),
		sstable.CreateFileName(path, newGen, "Index", "db"),
		sstable.CreateFileName(path, newGen, "Summary", "db"),
		sstable.CreateFileName(path, newGen, "Filter", "db"),
		sstable.CreateFileName(path, newGen, "Metadata", "db"),
		sstable.CreateFileName(path, newGen, "CompressionInfo", "db"))
	sstable.WriteTxtToFile(sstable.CreateFileName(path, newGen, "TOC", "txt"), toc_data)

	if err := os.Remove(sstable.CreateFileName(path, oldGen, "TOC", "txt")); err != nil {
		fmt.Printf("Failed to remove old TOC file for generation %d: %v\n", oldGen, err)
	}
}
