package sstable

import (
	"fmt"
	"os"
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
