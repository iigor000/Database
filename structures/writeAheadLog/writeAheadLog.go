package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/block_organization"
)

type WALRecord struct {
	CRC       uint32
	Timestamp int64
	Type      WALRecordType
	Tombstone bool
	KeySize   uint64
	ValueSize uint64
	Key       []byte
	Value     []byte
}

type WALRecordType byte

const (
	FULL WALRecordType = iota
	FIRST
	MIDDLE
	LAST
)

type WALSegment struct {
	filePath      string
	segmentNumber int
	writtenBlocks int
	isActive      bool
	blockManager  *block_organization.BlockManager
	cachedBM      *block_organization.CachedBlockManager
}

type WAL struct {
	config        *config.Config
	segments      []*WALSegment
	activeSegment *WALSegment
}

// Funkcija koja inicijalizuje wal
func SetOffWAL(cfg *config.Config) (*WAL, error) {
	if err := os.MkdirAll(cfg.Wal.WalDirectory, 0755); err != nil { // Ako ne postoji folder za wal segmente, kreiramo ga
		return nil, fmt.Errorf("error creating wal directory: %v", err)
	}
	files, err := os.ReadDir(cfg.Wal.WalDirectory) // Prolazimo kroz folder
	if err != nil {
		return nil, fmt.Errorf("error reading wal directory: %v", err)
	}

	var segments []*WALSegment
	segmentRegex := regexp.MustCompile(`^wal_(\d{4})\.log$`)
	for _, file := range files {
		if matches := segmentRegex.FindStringSubmatch(file.Name()); matches != nil { // Ako ime fajla odgovara regexu
			segmentNumber, _ := strconv.Atoi(matches[1]) // Uzimamo broj segmenta
			segments = append(segments, &WALSegment{     // Dodajemo segment u listu
				filePath:      filepath.Join(cfg.Wal.WalDirectory, file.Name()),
				segmentNumber: segmentNumber,                                                    // Redni broj segmenta
				isActive:      false,                                                            // Inicijalno nije aktivan
				blockManager:  &block_organization.BlockManager{BlockSize: cfg.Block.BlockSize}, // BlockManager za upravljanje blokovima
				cachedBM: &block_organization.CachedBlockManager{ // CachedBlockManager za kesiranje blokova
					BM: &block_organization.BlockManager{BlockSize: cfg.Block.BlockSize},
					C:  block_organization.NewBlockCache(cfg),
				},
			})

		}
	}
	sort.Slice(segments, func(i, j int) bool { // Sortiramo segmente po rednom br
		return segments[i].segmentNumber < segments[j].segmentNumber
	})

	wal := &WAL{
		config:   cfg,
		segments: segments,
	}
	if len(segments) == 0 { // Ako nema segmenata, kreiramo novi
		if err := wal.newSegment(); err != nil {
			return nil, fmt.Errorf("error creating new wal segment: %v", err)
		}
	} else {
		wal.activeSegment = segments[len(segments)-1] // Uzimamo poslednji segment
		wal.activeSegment.isActive = true
	}
	return wal, nil
}

// Funkcija koja kreira novi segment
func (w *WAL) newSegment() error {

	segmentNum := 1
	if len(w.segments) > 0 {
		segmentNum = w.segments[len(w.segments)-1].segmentNumber + 1
	}

	fileName := fmt.Sprintf("wal_%04d.log", segmentNum)
	filePath := filepath.Join(w.config.Wal.WalDirectory, fileName)

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating wal segment file: %v", err)
	}
	file.Close()

	newSegment := &WALSegment{
		filePath:      filePath,
		segmentNumber: segmentNum,
		isActive:      true,
		writtenBlocks: 0,
		blockManager:  &block_organization.BlockManager{BlockSize: w.config.Block.BlockSize},
		cachedBM: &block_organization.CachedBlockManager{
			BM: &block_organization.BlockManager{BlockSize: w.config.Block.BlockSize},
			C:  block_organization.NewBlockCache(w.config), // Prosleđujemo CEO config
		},
	}

	w.segments = append(w.segments, newSegment)
	w.activeSegment = newSegment
	return nil
}

func (w *WAL) Append(key, value []byte, tombstone bool) error {
	if err := w.changeActiveSegmentIfNeeded(); err != nil {
		return fmt.Errorf("error changing active segment: %v", err)
	}

	record := &WALRecord{
		Timestamp: time.Now().UnixNano(),
		Type:      FULL,
		Tombstone: tombstone,
		KeySize:   uint64(len(key)),
		ValueSize: uint64(len(value)),
		Key:       key,
		Value:     value,
	}

	serialized, err := record.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing record: %v", err)
	}

	// Izracunavanje broja blokova potrebnih za upis
	blockSize := w.config.Block.BlockSize
	blocksNeeded := len(serialized) / blockSize
	if len(serialized)%blockSize != 0 {
		blocksNeeded++
	}

	// proveri kapacitet segmenta
	if w.activeSegment.writtenBlocks+blocksNeeded > w.config.Wal.WalSegmentSize {
		w.activeSegment.isActive = false
		if err := w.newSegment(); err != nil {
			return fmt.Errorf("error creating new segment: %v", err)
		}
	}

	// Upisivanje serijalizovanog zapisa u aktivni segment
	_, err = w.activeSegment.cachedBM.Append(w.activeSegment.filePath, serialized)
	if err != nil {
		return err
	}

	w.activeSegment.writtenBlocks += blocksNeeded
	return nil
}

// Funkcija koja serijalizuje zapis
func (r *WALRecord) Serialize() ([]byte, error) {
	buffer := new(bytes.Buffer)
	combinedRecord := bytes.Join([][]byte{r.Key, r.Value}, nil)
	r.CRC = crc32.ChecksumIEEE(combinedRecord) // Racunanje CRC kontrolnog zbira

	if err := binary.Write(buffer, binary.BigEndian, r.CRC); err != nil {
		return nil, err
	}
	if err := binary.Write(buffer, binary.BigEndian, r.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Write(buffer, binary.BigEndian, byte(r.Type)); err != nil {
		return nil, err
	}
	if err := binary.Write(buffer, binary.BigEndian, r.Tombstone); err != nil {
		return nil, err
	}
	if err := binary.Write(buffer, binary.BigEndian, r.KeySize); err != nil {
		return nil, err
	}
	if err := binary.Write(buffer, binary.BigEndian, r.ValueSize); err != nil {
		return nil, err
	}
	if _, err := buffer.Write(r.Key); err != nil {
		return nil, err
	}
	if _, err := buffer.Write(r.Value); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil

}

// Funkcija koja menja aktivni segment ako je potrebno
func (w *WAL) changeActiveSegmentIfNeeded() error {
	if w.activeSegment.writtenBlocks < w.config.Wal.WalSegmentSize {
		return nil
	}
	w.activeSegment.isActive = false
	if err := w.newSegment(); err != nil {
		return fmt.Errorf("error creating new segment: %v", err)
	}
	return nil

}

// Funkcija koja cita zapise iz wal, poziva se prilikom oporavka sistema (rekonstrukcije mem strukture iz wal-a)
func (w *WAL) ReadRecords() ([]*WALRecord, error) {
	var records []*WALRecord
	var currentRecord *WALRecord
	var accumulatedData []byte

	for _, segment := range w.segments {
		fileInfo, err := os.Stat(segment.filePath)
		if err != nil {
			return nil, fmt.Errorf("stat failed: %v", err)
		}
		if fileInfo.Size() == 0 {
			continue
		}
		data, err := segment.cachedBM.Read(segment.filePath, 0)
		if err != nil {
			return nil, fmt.Errorf("error reading segment %s: %v", segment.filePath, err)
		}

		reader := bytes.NewReader(data)
		for reader.Len() > 0 {
			var crc uint32
			var timestamp int64
			var recordType byte
			var tombstoneByte byte
			var keySize, valueSize uint64

			// prcitaj zaglavlje zapisa
			if err := binary.Read(reader, binary.BigEndian, &crc); err != nil {
				if err == io.EOF {
					break
				}
				return nil, fmt.Errorf("error reading CRC: %v", err)
			}
			if err := binary.Read(reader, binary.BigEndian, &timestamp); err != nil {
				return nil, err
			}
			if err := binary.Read(reader, binary.BigEndian, &recordType); err != nil {
				return nil, err
			}
			if err := binary.Read(reader, binary.BigEndian, &tombstoneByte); err != nil {
				return nil, err
			}
			if err := binary.Read(reader, binary.BigEndian, &keySize); err != nil {
				return nil, err
			}
			if err := binary.Read(reader, binary.BigEndian, &valueSize); err != nil {
				return nil, err
			}

			// hendluj rekord na osnovu njegovog tipa
			switch WALRecordType(recordType) {
			case FULL:
				// Slucaj kada je zapis FULL
				record := &WALRecord{
					CRC:       crc,
					Timestamp: timestamp,
					Type:      FULL,
					Tombstone: tombstoneByte != 0,
					KeySize:   keySize,
					ValueSize: valueSize,
				}
				record.Key = make([]byte, keySize)
				if _, err := io.ReadFull(reader, record.Key); err != nil {
					return nil, fmt.Errorf("error reading key: %v", err)
				}
				record.Value = make([]byte, valueSize)
				if _, err := io.ReadFull(reader, record.Value); err != nil {
					return nil, fmt.Errorf("error reading value: %v", err)
				}

				// Verifikuj CRC kontrolni zbir
				combined := append(record.Key, record.Value...)
				if crc32.ChecksumIEEE(combined) != crc {
					return nil, fmt.Errorf("CRC mismatch")
				}
				records = append(records, record)

			case FIRST:
				// Slucaj kada je zapis fragmentiran i ovo je prvi deo
				currentRecord = &WALRecord{
					CRC:       crc,
					Timestamp: timestamp,
					Type:      FIRST,
					Tombstone: tombstoneByte != 0,
					KeySize:   keySize,
					ValueSize: valueSize,
				}
				accumulatedData = make([]byte, 0, keySize+valueSize)
				data := make([]byte, keySize+valueSize)
				if _, err := io.ReadFull(reader, data); err != nil {
					return nil, fmt.Errorf("error reading fragmented data: %v", err)
				}
				accumulatedData = append(accumulatedData, data...)

			case MIDDLE, LAST:
				// Nastavi fragmentaciju
				if currentRecord == nil {
					return nil, fmt.Errorf("orphaned MIDDLE/LAST record")
				}
				data := make([]byte, keySize+valueSize)
				if _, err := io.ReadFull(reader, data); err != nil {
					return nil, fmt.Errorf("error reading fragmented data: %v", err)
				}
				accumulatedData = append(accumulatedData, data...)

				if WALRecordType(recordType) == LAST {
					// Verifikuj CRC i duzinu fragmentiranog zapisa
					if crc32.ChecksumIEEE(accumulatedData) != currentRecord.CRC {
						return nil, fmt.Errorf("fragmented record CRC mismatch")
					}
					if uint64(len(accumulatedData)) != currentRecord.KeySize+currentRecord.ValueSize {
						return nil, fmt.Errorf("fragmented record size mismatch")
					}
					currentRecord.Key = accumulatedData[:currentRecord.KeySize]
					currentRecord.Value = accumulatedData[currentRecord.KeySize:]
					records = append(records, currentRecord)
					currentRecord = nil
					accumulatedData = nil
				}
			}
		}
	}

	if currentRecord != nil {
		return nil, fmt.Errorf("unfinished fragmented record at end of WAL")
	}

	return records, nil
}

// Funkcija koja brise segmente do odredjenog broja, poziva se nakon perzistiranja podataka u sstable
// sto se tice samog lwm potrebno je da se dinamicki racuna tokom rada sistema
// npr. nakon perzistiranja podataka u sstable/nakon brisanja podataka iz memtable, treba dodatno implementirati to
func (w *WAL) RemoveSegmentsUpTo(lowWaterMark int) error {
	var segmentsToKeep []*WALSegment
	var toDelete []*WALSegment

	// 1. Podeli segmente na one koje cuvamo i koje brisemo
	for _, segment := range w.segments {
		if segment.segmentNumber >= lowWaterMark {
			segmentsToKeep = append(segmentsToKeep, segment)
		} else {
			toDelete = append(toDelete, segment)
		}
	}

	// 2. Obrisi sve koji su ispod lowWaterMark-a
	for _, segment := range toDelete {
		if err := os.Remove(segment.filePath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("error removing segment %s: %v", segment.filePath, err)
		}
	}

	// 3. Preimenuj preostale segmente da budu u kontinuitetu (npr. 0001, 0002, ...)
	for i, segment := range segmentsToKeep {
		oldPath := segment.filePath
		newNumber := i + 1 // Segmenti pocinju od 1
		newName := fmt.Sprintf("wal_%04d.log", newNumber)
		newPath := filepath.Join(w.config.Wal.WalDirectory, newName)

		// Ako broj nije isti, preimenuj fajl i azuriraj segment
		if newNumber != segment.segmentNumber {
			if err := os.Rename(oldPath, newPath); err != nil {
				return fmt.Errorf("error renaming segment %d to %d: %v",
					segment.segmentNumber, newNumber, err)
			}
			segment.filePath = newPath
			segment.segmentNumber = newNumber
		}
	}
	// 4. Azuriraj listu segmenata
	w.segments = segmentsToKeep
	return nil
}
