package lsmtree

import (
	"fmt"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/structures/block_organization"
	"github.com/iigor000/database/structures/compression"
	"github.com/iigor000/database/structures/sstable"
)

// SSTableBuilder se koristi za gradnju SSTable-a
// Ovaj builder omogućava dodavanje zapisa i njihovo upisivanje u SSTable
type SSTableBuilder struct {
	level   int
	gen     int
	conf    *config.Config
	records []adapter.MemtableEntry
}

func NewSSTableBuilder(level, gen int, conf *config.Config) (*SSTableBuilder, error) {
	return &SSTableBuilder{
		level:   level,
		gen:     gen,
		conf:    conf,
		records: make([]adapter.MemtableEntry, 0),
	}, nil
}

func (b *SSTableBuilder) Write(entry adapter.MemtableEntry) error {
	// Možeš dodati validaciju ako želiš
	b.records = append(b.records, entry)
	return nil
}

// Finish kreira SSTable strukturu i upisuje je na disk zajedno sa svim potrebnim komponentama
func (b *SSTableBuilder) Finish(cbm *block_organization.CachedBlockManager, dict *compression.Dictionary) error {
	if len(b.records) == 0 {
		return fmt.Errorf("no entries to write")
	}

	sstable.BuildSSTable(b.records, b.conf, dict, cbm, b.gen, b.level)
	return nil
}
