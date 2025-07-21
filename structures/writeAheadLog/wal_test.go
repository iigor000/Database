package writeaheadlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iigor000/database/config"
)

// testiramo dodavanje i citanje full zapisa
func TestWAL_BasicAppendAndRead(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	cfg := &config.Config{
		Block: config.BlockConfig{
			BlockSize: 256,
		},
		Cache: config.CacheConfig{
			Capacity: 10,
		},
		Wal: config.WalConfig{
			WalDirectory:   tempDir,
			WalSegmentSize: 1024,
		},
	}

	wal, err := SetOffWAL(cfg)
	if err != nil {
		t.Fatalf("Failed to initialize WAL: %v", err)
	}

	key := []byte("key1")
	value := []byte("value1")
	tombstone := false

	if err := wal.Append(key, value, tombstone); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	records, err := wal.ReadRecords()
	if err != nil {
		t.Fatalf("ReadRecords failed: %v", err)
	}

	if len(records) == 0 {
		t.Fatal("No records found")
	}

	var matched *WALRecord
	for _, r := range records {
		if bytes.Equal(r.Key, key) {
			matched = r
			break
		}
	}
	if matched == nil {
		t.Fatalf("Record with key %s not found", key)
	}
	if !bytes.Equal(matched.Value, value) {
		t.Errorf("Value mismatch: want %s, got %s", value, matched.Value)
	}
	if matched.Tombstone != tombstone {
		t.Errorf("Tombstone mismatch: want %v, got %v", tombstone, matched.Tombstone)
	}
}

// testiramo dodavanje i citanje zapisa sa tombstone
func TestWAL_TombstoneAppendAndRead(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	cfg := &config.Config{
		Block: config.BlockConfig{
			BlockSize: 256,
		},
		Cache: config.CacheConfig{
			Capacity: 10,
		},
		Wal: config.WalConfig{
			WalDirectory:   tempDir,
			WalSegmentSize: 1024,
		},
	}

	wal, err := SetOffWAL(cfg)
	if err != nil {
		t.Fatalf("Failed to initialize WAL: %v", err)
	}

	key := []byte("key2")
	value := []byte("value2")
	tombstone := true

	if err := wal.Append(key, value, tombstone); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	records, err := wal.ReadRecords()
	if err != nil {
		t.Fatalf("ReadRecords failed: %v", err)
	}

	if len(records) == 0 {
		t.Fatal("No records found")
	}

	var matched *WALRecord
	for _, r := range records {
		if bytes.Equal(r.Key, key) {
			matched = r
			break
		}
	}
	if matched == nil {
		t.Fatalf("Record with key %s not found", key)
	}
	if !bytes.Equal(matched.Value, value) {
		t.Errorf("Value mismatch: want %s, got %s", value, matched.Value)
	}
	if matched.Tombstone != tombstone {
		t.Errorf("Tombstone mismatch: want %v, got %v", tombstone, matched.Tombstone)
	}
}

// testiramo dodavanje i citanje zapisa sa praznim value
func TestWAL_EmptyValueAppendAndRead(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	cfg := &config.Config{
		Block: config.BlockConfig{
			BlockSize: 256,
		},
		Cache: config.CacheConfig{
			Capacity: 10,
		},
		Wal: config.WalConfig{
			WalDirectory:   tempDir,
			WalSegmentSize: 1024,
		},
	}

	wal, err := SetOffWAL(cfg)
	if err != nil {
		t.Fatalf("Failed to initialize WAL: %v", err)
	}

	key := []byte("key3")
	value := []byte("")
	tombstone := false

	if err := wal.Append(key, value, tombstone); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	records, err := wal.ReadRecords()
	if err != nil {
		t.Fatalf("ReadRecords failed: %v", err)
	}

	if len(records) == 0 {
		t.Fatal("No records found")
	}

	var matched *WALRecord
	for _, r := range records {
		if bytes.Equal(r.Key, key) {
			matched = r
			break
		}
	}
	if matched == nil {
		t.Fatalf("Record with key %s not found", key)
	}
	if !bytes.Equal(matched.Value, value) {
		t.Errorf("Value mismatch: want %s, got %s", value, matched.Value)
	}
	if matched.Tombstone != tombstone {
		t.Errorf("Tombstone mismatch: want %v, got %v", tombstone, matched.Tombstone)
	}
}

// Ovaj test proverava da li se segmenti pravilno rotiraju kada se dostigne
// maksimalna velicina
func TestWAL_SegmentRotation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	cfg := &config.Config{
		Block: config.BlockConfig{
			BlockSize: 128, // Mali blokovi za brzu rotaciju
		},
		Cache: config.CacheConfig{
			Capacity: 10,
		},
		Wal: config.WalConfig{
			WalDirectory:   tempDir,
			WalSegmentSize: 2, // Rotiraj posle 2 bloka
		},
	}

	wal, err := SetOffWAL(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Upis dovoljno podataka da izazove rotaciju
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := wal.Append(key, value, false); err != nil {
			t.Fatal(err)
		}
	}

	if len(wal.segments) < 2 {
		t.Errorf("Expected at least 2 segments, got %d", len(wal.segments))
	}
}

// Ovaj test proverava da li se veliki zapisi pravilno fragmentiraju
// kada su veci od velicine bloka
func TestWAL_LargeRecord(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	cfg := &config.Config{
		Block: config.BlockConfig{
			BlockSize: 128, // Mali blok za testiranje fragmentacije
		},
		Cache: config.CacheConfig{
			Capacity: 10,
		},
		Wal: config.WalConfig{
			WalDirectory:   tempDir,
			WalSegmentSize: 10,
		},
	}

	wal, err := SetOffWAL(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Veliki zapis (veci od velicine bloka)
	largeValue := make([]byte, 500)
	for i := range largeValue {
		largeValue[i] = byte('A' + (i % 26))
	}

	if err := wal.Append([]byte("largeKey"), largeValue, false); err != nil {
		t.Fatal(err)
	}

	records, err := wal.ReadRecords()
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(records))
	}

	if string(records[0].Key) != "largeKey" {
		t.Errorf("Key mismatch: want 'largeKey', got '%s'", records[0].Key)
	}

	if len(records[0].Value) != len(largeValue) {
		t.Errorf("Value length mismatch: want %d, got %d", len(largeValue), len(records[0].Value))
	}
}

// Ovaj test proverava da li se segmenti pravilno uklanjaju
func TestWAL_RemoveSegments(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	cfg := &config.Config{
		Block: config.BlockConfig{
			BlockSize: 256,
		},
		Cache: config.CacheConfig{
			Capacity: 10,
		},
		Wal: config.WalConfig{
			WalDirectory:   tempDir,
			WalSegmentSize: 1, // Mali segment za testiranje
		},
	}

	wal, err := SetOffWAL(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Dodaj nekoliko zapisa da kreira vise segmenata
	for i := 0; i < 3; i++ {
		if err := wal.Append([]byte(fmt.Sprintf("key%d", i)), []byte("value"), false); err != nil {
			t.Fatal(err)
		}
	}

	// Proveri inicijalni broj segmenata
	if len(wal.segments) < 2 {
		t.Fatalf("Expected at least 2 segments, got %d", len(wal.segments))
	}

	// Obrisi segmente pre broja 2
	if err := wal.RemoveSegmentsUpTo(2); err != nil {
		t.Fatal(err)
	}

	// Proveri da je ostao bar jedan segment
	if len(wal.segments) == 0 {
		t.Error("Expected at least 1 segment remaining")
	}

	// Proveri da li su svi preostali segmenti preimenovani redom: wal_0001.log, wal_0002.log, ...
	for i, segment := range wal.segments {
		expectedName := fmt.Sprintf("wal_%04d.log", i+1)
		expectedPath := filepath.Join(tempDir, expectedName)
		if segment.filePath != expectedPath {
			t.Errorf("Expected segment path %s, got %s", expectedPath, segment.filePath)
		}
		if segment.segmentNumber != i+1 {
			t.Errorf("Expected segment number %d, got %d", i+1, segment.segmentNumber)
		}
	}
}

// Ovaj test proverava serijalizaciju i deserijalizaciju zapisa
func TestWAL_RecordSerialization(t *testing.T) {
	now := time.Now().UnixNano()
	record := &WALRecord{
		CRC:       12345,
		Timestamp: now,
		Type:      FULL,
		Tombstone: true,
		KeySize:   3,
		ValueSize: 5,
		Key:       []byte("key"),
		Value:     []byte("value"),
	}

	data, err := record.Serialize()
	if err != nil {
		t.Fatal(err)
	}

	// Proveri osnovne karakteristike serijalizovanih podataka
	if len(data) < 30 { // Minimalna velicina zaglavlja + key/value
		t.Errorf("Serialized data too small: %d bytes", len(data))
	}

	// Test roundtrip
	newRecord, err := DeserializeWALRecord(data)
	if err != nil {
		t.Fatal(err)
	}

	if newRecord.CRC != record.CRC {
		t.Errorf("CRC mismatch: want %d, got %d", record.CRC, newRecord.CRC)
	}
	if string(newRecord.Key) != "key" {
		t.Errorf("Key mismatch: want 'key', got '%s'", newRecord.Key)
	}
}

// Pomocna funkcija za deserijalizaciju (potrebna za testove)
func DeserializeWALRecord(data []byte) (*WALRecord, error) {
	reader := bytes.NewReader(data)
	record := &WALRecord{}

	if err := binary.Read(reader, binary.BigEndian, &record.CRC); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.BigEndian, &record.Timestamp); err != nil {
		return nil, err
	}
	var recordType byte
	if err := binary.Read(reader, binary.BigEndian, &recordType); err != nil {
		return nil, err
	}
	record.Type = WALRecordType(recordType)
	var tombstoneByte byte
	if err := binary.Read(reader, binary.BigEndian, &tombstoneByte); err != nil {
		return nil, err
	}
	record.Tombstone = tombstoneByte != 0
	if err := binary.Read(reader, binary.BigEndian, &record.KeySize); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.BigEndian, &record.ValueSize); err != nil {
		return nil, err
	}

	record.Key = make([]byte, record.KeySize)
	if _, err := reader.Read(record.Key); err != nil {
		return nil, err
	}

	record.Value = make([]byte, record.ValueSize)
	if _, err := reader.Read(record.Value); err != nil {
		return nil, err
	}

	return record, nil
}
