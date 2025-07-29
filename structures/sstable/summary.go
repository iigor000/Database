package sstable

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/block_organization"
)

// SummaryBlock struktura je skup IndexRecord-a
// FirstKey je ključ prvog zapisa u bloku
type SummaryRecord struct {
	FirstKey        []byte
	IndexOffset     int // Offset u Index segmentu gde se nalazi ovaj blok
	NumberOfRecords int
}

type Summary struct {
	Records     []SummaryRecord
	FirstKey    []byte // Prvi kljuc u Summary
	LastKey     []byte // Poslednji kljuc u Summary
	SummaryFile File
}

func (sb *Summary) WriteSummary(path string, conf *config.Config, cbm *block_organization.CachedBlockManager) error {
	// Prvo upisujemo header
	header, err := sb.SerializeHeader()
	if err != nil {
		return fmt.Errorf("failed to serialize summary header: %w", err)
	}
	bn, err := cbm.AppendBlock(path, header)
	if err != nil {
		return fmt.Errorf("failed to write summary header: %w", err)
	}
	sb.SummaryFile.Path = path

	sb.SummaryFile.Offset = int64(bn * conf.Block.BlockSize)

	for _, record := range sb.Records {
		bn, err := record.WriteSummaryRecord(path, cbm)
		if err != nil {
			return err
		}

		sb.SummaryFile.SizeOnDisk = int64(bn * conf.Block.BlockSize)
	}
	serializedData, _ := sb.Records[len(sb.Records)-1].Serialize()
	for i := 1; i < 10001; i++ {
		if len(serializedData) < i*conf.Block.BlockSize {
			sb.SummaryFile.SizeOnDisk += int64(i * conf.Block.BlockSize)
			break
		}
	}
	sb.SummaryFile.SizeOnDisk -= sb.SummaryFile.Offset
	return nil
}

func (sr *SummaryRecord) WriteSummaryRecord(path string, bm *block_organization.CachedBlockManager) (int, error) {
	serializedData, _ := sr.Serialize()
	bn, err := bm.Append(path, serializedData)
	if err != nil {
		return -1, err
	}
	return bn, nil
}

func (sr *SummaryRecord) Serialize() ([]byte, error) {
	serializedData := make([]byte, 0)

	// Prvo upisujemo duzinu kljuca
	serializedData = append(serializedData, byte(len(sr.FirstKey)))
	serializedData = append(serializedData, sr.FirstKey...)

	// Zatim upisujemo IndexOffset i NumberOfRecords
	serializedData = append(serializedData, byte(sr.IndexOffset>>24), byte(sr.IndexOffset>>16), byte(sr.IndexOffset>>8), byte(sr.IndexOffset))
	serializedData = append(serializedData, byte(sr.NumberOfRecords>>24), byte(sr.NumberOfRecords>>16), byte(sr.NumberOfRecords>>8), byte(sr.NumberOfRecords))

	return serializedData, nil
}

func (s *Summary) SerializeHeader() ([]byte, error) {
	serializedData := make([]byte, 0)

	// Prvo upisujemo duzinu FirstKey
	serializedData = append(serializedData, byte(len(s.FirstKey)))
	serializedData = append(serializedData, s.FirstKey...)

	// Zatim upisujemo duzinu LastKey
	serializedData = append(serializedData, byte(len(s.LastKey)))
	serializedData = append(serializedData, s.LastKey...)

	return serializedData, nil
}

func ReadSummary(path string, conf *config.Config, startOffset, endOffset int64, bm *block_organization.CachedBlockManager) (*Summary, error) {
	block_num := int(startOffset / int64(conf.Block.BlockSize)) // Pocinjemo od bloka koji sadrzi startOffset
	end_block := int(endOffset / int64(conf.Block.BlockSize))   // Kraj bloka koji sadrzi endOffset
	if endOffset <= startOffset {
		end_block = -1 // Kraj bloka koji sadrzi endOffset
	}
	summary := &Summary{}
	data, err := bm.ReadBlock(path, block_num)
	if err != nil {
		if err.Error() == "EOF" {
			return nil, fmt.Errorf("summary file is empty: %w", err)
		}
		return nil, fmt.Errorf("error reading summary file: %w", err)
	}

	if err := summary.DeserializeHeader(data); err != nil {
		return nil, fmt.Errorf("failed to deserialize summary header: %w", err)
	}
	block_num++
	for {
		block, err := bm.Read(path, block_num)
		if err != nil {
			if err.Error() == "EOF" || strings.Contains(err.Error(), "EOF") {
				break // Kraj fajla
			}
			return nil, err
		}

		if len(block) == 0 {
			break // Kraj fajla
		}
		block_num1 := block_num
		i := 1
		for {
			if len(block)+(i*1) <= i*conf.Block.BlockSize {
				block_num1 = i + block_num
				break
			}
			i++
		}
		if end_block != -1 && block_num1 > end_block {
			break // Dostigli smo kraj bloka koji nas zanima
		}
		sr := &SummaryRecord{}
		err = sr.Deserialize(block)
		if err != nil {
			return nil, err
		}
		summary.Records = append(summary.Records, *sr)
		block_num = block_num1
	}
	summary.SummaryFile = File{
		Path:       path,
		Offset:     startOffset,
		SizeOnDisk: int64(block_num)*int64(conf.Block.BlockSize) - startOffset,
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
	if len(data) < offset+8 {
		return fmt.Errorf("data too short to read index offset and number of records: %d bytes", len(data))
	}
	sr.IndexOffset = int(data[offset])<<24 | int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
	sr.NumberOfRecords = int(data[offset+4])<<24 | int(data[offset+5])<<16 | int(data[offset+6])<<8 | int(data[offset+7])

	return nil
}

func (s *Summary) DeserializeHeader(data []byte) error {
	if len(data) < 2 {
		return fmt.Errorf("data too short to deserialize Summary header: %d bytes", len(data))
	}

	firstKeySize := int(data[0])
	if len(data) < 1+firstKeySize+1 {
		return fmt.Errorf("data too short to read first key size and first key: %d bytes", len(data))
	}
	s.FirstKey = data[1 : 1+firstKeySize]

	lastKeySize := int(data[1+firstKeySize])
	if len(data) < 1+firstKeySize+1+lastKeySize {
		return fmt.Errorf("data too short to read last key size and last key: %d bytes", len(data))
	}
	s.LastKey = data[1+firstKeySize+1 : 1+firstKeySize+1+lastKeySize]

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
		return SummaryRecord{}, fmt.Errorf("no summary record found for key: %s", key)
	}
	return s.Records[resultIdx], nil
}

// ReadSummaryMinMax čita prvi i poslednji ključ iz Summary fajla
// Konstruiše putanju do Summary/SSTable fajla na osnovu konfiguracije i nivoa/generacije
func ReadSummaryMinMax(level int, gen int, conf *config.Config, cbm *block_organization.CachedBlockManager) ([]byte, []byte, error) {
	bm := block_organization.NewBlockManager(conf)
	path := fmt.Sprintf("%s/%d/%d", conf.SSTable.SstableDirectory, level, gen)

	var blockNum int

	if conf.SSTable.SingleFile {
		path = CreateFileName(path, gen, "SSTable", "db")
		offsets, err := ReadOffsetsFromFile(path, conf, cbm)
		if err != nil {
			return nil, nil, fmt.Errorf("error reading offsets from file %s: %w", path, err)
		}
		blockNum = int(offsets["Summary"] / int64(conf.Block.BlockSize))
	} else {
		path = CreateFileName(path, gen, "Summary", "db")
		blockNum = 0
	}

	data, err := bm.ReadBlock(path, blockNum)
	if err != nil {
		if err.Error() == "EOF" {
			return nil, nil, fmt.Errorf("summary file is empty: %w", err)
		}
		return nil, nil, fmt.Errorf("error reading summary file: %w", err)
	}

	summary := &Summary{}
	if err := summary.DeserializeHeader(data); err != nil {
		return nil, nil, fmt.Errorf("failed to deserialize summary header: %w", err)
	}

	return summary.FirstKey, summary.LastKey, nil
}
