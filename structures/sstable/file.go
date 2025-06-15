package sstable

import "fmt"

func CreateFileName(path string, gen int, element string, ext string) string {
	return fmt.Sprintf("%s/usertable-%06d-%s.%s", path, gen, element, ext)
}

type BinFile struct {
	FileName    string
	StartOffset int64
	Size        int64
}
