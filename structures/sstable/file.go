package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/block_organization"
)

type File struct {
	Path       string // Putanja do fajla
	Offset     int64  // Offset u fajlu gde su podaci upisani
	SizeOnDisk int64  // Velicina fajla na disku
}

// FileExists proverava da li fajl postoji na datoj putanji
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil || !os.IsNotExist(err)
}

func CreateFileName(path string, gen int, element string, ext string) string {
	return fmt.Sprintf("%s/usertable-%06d-%s.%s", path, gen, element, ext)
}

func CreateDirectory(path string) error {
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory %s: %w", path, err)
	}
	return nil
}

// CreateDirectoryIfNotExists kreira direktorijum ako ne postoji
// Ako direktorijum vec postoji, brise sve iz njega
// i ostavlja prazan direktorijum
func CreateDirectoryIfNotExists(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return CreateDirectory(path)
	}
	// Ako direktorijum postoji, brisemo sve iz njega
	files, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("failed to read directory %s: %w", path, err)
	}
	for _, file := range files {
		err := os.RemoveAll(fmt.Sprintf("%s/%s", path, file.Name()))
		if err != nil {
			return fmt.Errorf("failed to remove file %s: %w", file.Name(), err)
		}
	}
	return nil
}

func WriteTxtToFile(path string, content string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", path, err)
	}
	defer file.Close()

	_, err = file.WriteString(content)
	if err != nil {
		return fmt.Errorf("failed to write to file %s: %w", path, err)
	}

	return nil
}

// CalculateDataSize izracunava velicinu podataka u SSTable
func CalculateDataSize(path string, conf *config.Config) int64 {
	if conf.SSTable.SingleFile {
		info, err := os.Stat(path)
		if err != nil {
			fmt.Printf("failed to stat file '%s': %v\n", path, err)
			return 0
		}
		if !info.IsDir() {
			return info.Size()
		}
	}

	var totalSize int64 = 0

	// Ako nije SingleFile, path je direktorijum, saberi veličine svih fajlova
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})
	if err != nil {
		fmt.Printf("failed to walk directory '%s': %v\n", path, err)
		return 0
	}
	return totalSize
}

// ReadOffsetsFromFile cita offsete iz fajla
func ReadOffsetsFromFile(path string, conf *config.Config, bm *block_organization.CachedBlockManager) (map[string]int64, error) {
	offsets := make(map[string]int64)
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
