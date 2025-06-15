package sstable

// SummaryRecord struktura je jedan zapis u Summary segmentu SSTable-a
type SummaryRecord struct {
	Key    []byte
	Offset int64
}

// SummaryBlock struktura je skup SummaryRecord-a
// FirstKey je ključ prvog zapisa u bloku
// LastKey je ključ poslednjeg zapisa u bloku
type SummaryBlock struct {
	Records         []SummaryRecord
	FirstKey        []byte
	LastKey         []byte
	NumberOfRecords int
}
