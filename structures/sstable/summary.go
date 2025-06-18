package sstable

import (
	"github.com/iigor000/database/config"
)

// SummaryBlock struktura je skup IndexRecord-a
// FirstKey je ključ prvog zapisa u bloku
// LastKey je ključ poslednjeg zapisa u bloku
type SummaryRecord struct {
	Records         []IndexRecord
	FirstKey        []byte
	LastKey         []byte
	NumberOfRecords int
}

type SummaryBlock struct {
	Records []SummaryRecord
}

func (sb *SummaryBlock) WriteSummary(path string, conf *config.Config) error {

	return nil
}
