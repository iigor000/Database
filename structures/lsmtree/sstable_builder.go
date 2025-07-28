package lsmtree

import (
	"fmt"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/structures/block_organization"
	"github.com/iigor000/database/structures/bloomfilter"
	"github.com/iigor000/database/structures/merkle"
	"github.com/iigor000/database/structures/sstable"
)

// SSTableBuilder se koristi za gradnju SSTable-a
// Ovaj builder omogućava dodavanje zapisa i njihovo upisivanje u SSTable
type SSTableBuilder struct {
	level   int
	gen     int
	conf    *config.Config
	records []*adapter.MemtableEntry
}

func NewSSTableBuilder(level, gen int, conf *config.Config) (*SSTableBuilder, error) {
	return &SSTableBuilder{
		level:   level,
		gen:     gen,
		conf:    conf,
		records: make([]*adapter.MemtableEntry, 0),
	}, nil
}

func (b *SSTableBuilder) Write(entry *adapter.MemtableEntry) error {
	// Možeš dodati validaciju ako želiš
	b.records = append(b.records, entry)
	return nil
}

// Finish kreira SSTable strukturu i upisuje je na disk zajedno sa svim potrebnim komponentama
func (b *SSTableBuilder) Finish(cbm *block_organization.CachedBlockManager) error {
	if len(b.records) == 0 {
		return fmt.Errorf("no entries to write")
	}

	// Pripremi SSTable strukturu
	newSST := sstable.NewEmptySSTable(b.conf, b.level, b.gen)

	// Popuni Data sekciju
	db := &sstable.Data{}
	for _, entry := range b.records {
		rec := sstable.NewDataRecord(entry.Key, entry.Value, entry.Timestamp, entry.Tombstone)
		db.Records = append(db.Records, rec)
		if b.conf.SSTable.UseCompression {
			newSST.CompressionKey.Add(rec.Key)
		}
	}
	newSST.Data = db

	// Napravi index
	ib := &sstable.Index{}
	for _, rec := range db.Records {
		ir := sstable.NewIndexRecord(rec.Key, rec.Offset)
		ib.Records = append(ib.Records, ir)
	}
	newSST.Index = ib

	// Napravi summary
	summaryLevel := b.conf.SSTable.SummaryLevel
	sb := &sstable.Summary{}
	for i := 0; i < len(ib.Records); i += summaryLevel {
		// Pravimo summary sa onoliko koliko je ostalo
		end := i + summaryLevel
		if end > len(ib.Records) {
			end = len(ib.Records)
		}

		sr := sstable.SummaryRecord{
			FirstKey:        ib.Records[i].Key,
			IndexOffset:     ib.Records[i].IndexOffset,
			NumberOfRecords: end - i,
		}
		sb.Records = append(sb.Records, sr)
	}
	newSST.Summary = sb

	// BloomFilter
	fb := bloomfilter.MakeBloomFilter(len(db.Records), 0.5)
	for _, rec := range db.Records {
		fb.Add(rec.Key)
		if !rec.Tombstone {
			fb.Add(rec.Value)
		}
	}
	newSST.Filter = fb

	// Merkle stablo
	data := make([][]byte, len(db.Records))
	for i, rec := range db.Records {
		d := rec.Key
		if !rec.Tombstone {
			d = append(d, rec.Value...)
		}
		data[i] = d
	}
	newSST.Metadata = merkle.NewMerkleTree(data)

	// Direktorijum za novi SSTable
	dir := fmt.Sprintf("%s/%d", b.conf.SSTable.SstableDirectory, b.level)
	err := sstable.CreateDirectoryIfNotExists(dir)
	if err != nil {
		return fmt.Errorf("failed to create directory for SSTable: %w", err)
	}

	// Upis SSTable na disk
	sstable.WriteSSTable(newSST, dir, b.conf, cbm)

	return nil
}
