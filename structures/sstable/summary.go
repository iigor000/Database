package sstable

import (
	"bytes"
	"fmt"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/block_organization"
)

// SummaryBlock struktura je skup IndexRecord-a
// FirstKey je ključ prvog zapisa u bloku
// LastKey je ključ poslednjeg zapisa u bloku
type SummaryRecord struct {
	FirstKey        []byte
	LastKey         []byte
	IndexOffset     int // Offset u Index segmentu gde se nalazi ovaj blok
	NumberOfRecords int
}

type Summary struct {
	Records []SummaryRecord
}

func (sb *Summary) WriteSummary(path string, conf *config.Config) error {
	bm := block_organization.NewBlockManager(conf)
	for _, record := range sb.Records {
		err := record.WriteSummaryRecord(path, bm)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sr *SummaryRecord) WriteSummaryRecord(path string, bm *block_organization.BlockManager) error {
	serializedData, _ := sr.Serialize()
	_, err := bm.AppendBlock(path, serializedData)
	if err != nil {
		return err
	}
	return nil
}

func (sr *SummaryRecord) Serialize() ([]byte, error) {
	serializedData := make([]byte, 0)

	// Prvo upisujemo duzinu kljuca
	serializedData = append(serializedData, byte(len(sr.FirstKey)))
	serializedData = append(serializedData, sr.FirstKey...)
	serializedData = append(serializedData, byte(len(sr.LastKey)))
	serializedData = append(serializedData, sr.LastKey...)

	// Zatim upisujemo IndexOffset i NumberOfRecords
	serializedData = append(serializedData, byte(sr.IndexOffset>>24), byte(sr.IndexOffset>>16), byte(sr.IndexOffset>>8), byte(sr.IndexOffset))
	serializedData = append(serializedData, byte(sr.NumberOfRecords>>24), byte(sr.NumberOfRecords>>16), byte(sr.NumberOfRecords>>8), byte(sr.NumberOfRecords))

	return serializedData, nil
}

func ReadSummary(path string, conf *config.Config) (*Summary, error) {
	bm := block_organization.NewBlockManager(conf)
	block_num := 0 // Pocinjemo od prvog bloka
	summary := &Summary{}

	for {
		block, err := bm.ReadBlock(path, block_num)
		if err != nil {
			if err.Error() == "EOF" {
				break // Kraj fajla
			}
			return nil, err
		}

		if len(block) == 0 {
			break // Kraj fajla
		}

		sr := &SummaryRecord{}
		err = sr.Deserialize(block)
		if err != nil {
			return nil, err
		}
		summary.Records = append(summary.Records, *sr)
		block_num++
	}

	return summary, nil
}

func (sr *SummaryRecord) Deserialize(data []byte) error {
	if len(data) < 10 {
		return fmt.Errorf("data too short to deserialize SummaryRecord: %d bytes", len(data))
	}

	keyLen := int(data[0])
	if len(data) < 1+keyLen+1 {
		return fmt.Errorf("data too short to read key length and first key: %d bytes", len(data))
	}
	sr.FirstKey = data[1 : 1+keyLen]

	offset := 1 + keyLen
	lastKeyLen := int(data[offset])
	if len(data) < offset+1+lastKeyLen+8 {
		return fmt.Errorf("data too short to read last key length and last key: %d bytes", len(data))
	}
	sr.LastKey = data[offset+1 : offset+1+lastKeyLen]

	offset += 1 + lastKeyLen
	sr.IndexOffset = int(data[offset])<<24 | int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
	sr.NumberOfRecords = int(data[offset+4])<<24 | int(data[offset+5])<<16 | int(data[offset+6])<<8 | int(data[offset+7])

	return nil
}

// Pomocna funkcija za citanje SummaryRecord-a sa prefiksom
// (Summary je vec ucitan iz fajla)
func (s *Summary) FindSummaryRecordWithKey(key string) (SummaryRecord, error) {
	left, right := 0, len(s.Records)-1
	resultIdx := -1

	for left <= right {
		mid := (left + right) / 2
		if bytes.Compare(s.Records[mid].FirstKey, []byte(key)) <= 0 {
			// Kandidat, ali tražimo još veći koji je <= key
			resultIdx = mid
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	if resultIdx == -1 {
		//Proveri da li prvi record sadrzi kljuc
		if bytes.HasPrefix(s.Records[0].FirstKey, []byte(key)) {
			return s.Records[0], nil
		}
		println("No summary record found for key:", key)
		return SummaryRecord{}, fmt.Errorf("no summary record found for key: %s", key)
	}
	return s.Records[resultIdx], nil
}
