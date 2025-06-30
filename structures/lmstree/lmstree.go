package lmstree

import (
	"bytes"
	"container/heap"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/block_organization"
	"github.com/iigor000/database/structures/cache"
	"github.com/iigor000/database/structures/memtable"
	"github.com/iigor000/database/structures/sstable"
)

type LSMTree struct {
	Memtables    *memtable.Memtables
	SSTables     [][]*sstable.SSTable
	Cache        *cache.Cache
	BlockManager *block_organization.BlockManager
}

// Kreira novo LSM stablo
func NewLSMTree(cfg *config.Config) *LSMTree {
	return &LSMTree{
		Cache:        cache.NewCache(cfg),
		SSTables:     [][]*sstable.SSTable{},
		BlockManager: block_organization.NewBlockManager(cfg),
		Memtables:    memtable.NewMemtables(cfg),
	}
}

// Dodaje ključ i vrednost u Memtable i pokreće algoritam za kompakciju ako je potrebno
func (l *LSMTree) Put(conf *config.Config, key []byte, value []byte) {
	// Prvo dodaj u Memtable
	flushed := l.Memtables.Update(key, value, time.Now().UnixNano(), false)

	// Zatim ubaci iz Cache
	l.Cache.Put(hex.EncodeToString(key), value)

	if flushed {
		l.Compact(conf, 0) // Početni nivo kompakcije je 0
	}
}

// Traži ključ u Memtable, Cache i SStables
func (l *LSMTree) Get(key []byte) ([]byte, bool) {

	// Prvo proveri u Memtables
	if value, exists := l.Memtables.Search(key); exists {
		return value, true
	}

	// Proveri u Cache
	if value, exists := l.Cache.Get(hex.EncodeToString(key)); exists {
		return value, true
	}

	// Proveri u SSTable-ima (idemo od nižeg ka višim nivoima jer se u nižim nivoima nalaze noviji podaci)
	for _, level := range l.SSTables {
		for _, sstable := range level {
			// Proveri Bloom filter pre pretrage
			if !sstable.Filter.Read(key) {
				continue // Ako Bloom filter ne sadrži ključ, pređi na naredni SSTable
			}

			// Pregledaj summary i index samo u SSTable-ovima koji postoje trenutno
			if sst.ContainsKey(key) {
				value, err := sst.ReadValue(key, l.BlockManager)
				if err == nil {
					l.Cache.Put(hex.EncodeToString(key), value)
					return value, true
				}
			}
		}
	}

	return nil, false
}

func (l *LSMTree) Delete(conf *config.Config, key []byte) {
	l.Put(conf, key, nil)
	// prilikom flush-a, tombstone će se prepisati na prethodnu vrednost u disku jer ima veći timestamp
	// Ako se pokuša Get() sa tim ključem, dobiće vrednost iz Memtable-a, što će biti nil, što znači da je obrisano
}

func (l *LSMTree) Compact(conf *config.Config, level int) {
	if conf.LSMTree.CompactionAlgorithm == "size_tiered" {
		l.sizeTieredCompaction(conf, level)
	} else if conf.LSMTree.CompactionAlgorithm == "leveled" {
		l.leveledCompaction(conf, level)
	}
}

type SSTableIterator struct {
	records []sstable.DataRecord
	index   int
}

func NewSSTableIterator(sst *sstable.SSTable) *SSTableIterator {
	return &SSTableIterator{
		records: sst.Data.Records,
		index:   0,
	}
}

func (it *SSTableIterator) HasNext() bool {
	return it.index < len(it.records)
}

func (it *SSTableIterator) Peek() *sstable.DataRecord {
	if !it.HasNext() {
		return nil
	}
	return &it.records[it.index]
}

func (it *SSTableIterator) Next() *sstable.DataRecord {
	if !it.HasNext() {
		return nil
	}
	rec := &it.records[it.index]
	it.index++
	return rec
}

type IteratorEntry struct {
	record   *sstable.DataRecord
	iterator *SSTableIterator
}

type IteratorHeap []IteratorEntry

func (h IteratorHeap) Len() int { return len(h) }
func (h IteratorHeap) Less(i, j int) bool {
	// Sortiraj po ključu; ako su ključevi isti, sortiraj po timestamp-u opadajuće (noviji prvi)
	if cmp := bytes.Compare(h[i].record.Key, h[j].record.Key); cmp != 0 {
		return cmp < 0
	}
	return h[i].record.Timestamp > h[j].record.Timestamp
}
func (h IteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *IteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(IteratorEntry))
}

func (h *IteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	elem := old[n-1]
	*h = old[:n-1]
	return elem
}

func DeleteSSTableFiles(conf *config.Config, sst *sstable.SSTable) {
	base := fmt.Sprintf("%s/%d", conf.SSTable.SstableDirectory, sst.Gen)
	os.RemoveAll(base)
}

func advanceIteratorAndPush(it *SSTableIterator, h *IteratorHeap) {
	it.Next()
	if it.HasNext() {
		next := it.Peek()
		heap.Push(h, IteratorEntry{record: next, iterator: it})
	}
}

func MergeSSTablesRecords(tables []*sstable.SSTable) []*sstable.DataRecord {
	var entries []IteratorEntry

	for _, table := range tables {
		it := NewSSTableIterator(table)
		if it.HasNext() {
			entries = append(entries, IteratorEntry{
				record:   it.Peek(),
				iterator: it,
			})
		}
	}
	h := IteratorHeap(entries)
	heap.Init(&h)

	var mergedRecords []*sstable.DataRecord
	seen := make(map[string]bool)

	for h.Len() > 0 {
		entry := heap.Pop(&h).(IteratorEntry)
		rec := entry.record
		it := entry.iterator

		keyStr := hex.EncodeToString(rec.Key)

		// Preskoči duplikate
		// Ako je ključ već viđen, preskoči ga i nastavi sa sledećim
		if seen[keyStr] {
			advanceIteratorAndPush(it, &h)
			continue
		}

		// Ažuriraj najnoviju verziju za ovaj ključ
		latest := rec
		seen[keyStr] = true

		// Proveri sve ostale iteratore u gomili koji imaju isti ključ
		for h.Len() > 0 {
			next := (h)[0] // pogledaj prvi element bez uklanjanja
			if bytes.Equal(next.record.Key, rec.Key) {
				nextEntry := heap.Pop(&h).(IteratorEntry)
				if nextEntry.record.Timestamp > latest.Timestamp {
					latest = nextEntry.record
				}
				advanceIteratorAndPush(nextEntry.iterator, &h)
			} else {
				break
			}
		}

		// Ako je najnovija verzija tombstone, preskoči je
		if !latest.Tombstone {
			mergedRecords = append(mergedRecords, latest)
		}

		advanceIteratorAndPush(it, &h)
	}

	return mergedRecords
}

func BuildSSTableFromRecords(mergedRecords []*sstable.DataRecord, conf *config.Config, newGen int) *sstable.SSTable {
	var newSST sstable.SSTable
	newSST.UseCompression = conf.SSTable.UseCompression
	newSST.Gen = newGen

	dir := fmt.Sprintf("%s/%d", conf.SSTable.SstableDirectory, newGen)
	if err := sstable.CreateDirectoryIfNotExists(dir); err != nil {
		panic("Error creating directory for SSTable: " + err.Error())
	}

	newSST.Data, newSST.CompressionKey = sstable.buildDataFromRecords(mergedRecords, conf, newGen, dir)
	newSST.Index = sstable.buildIndex(conf, newGen, dir, newSST.Data)
	newSST.Summary = sstable.buildSummary(conf, newSST.Index)
	newSST.Filter = sstable.buildBloomFilter(conf, newGen, dir, newSST.Data)

	if newSST.UseCompression && newSST.CompressionKey != nil {
		dictPath := sstable.CreateFileName(dir, newGen, "Dictionary", "db")
		newSST.CompressionKey.Write(dictPath)
	}

	newSST.Metadata = sstable.buildMetadata(conf, newGen, dir, newSST.Data)

	tocPath := sstable.CreateFileName(dir, newGen, "TOC", "txt")
	tocData := fmt.Sprintf(
		"Generation: %d\nData: %s\nIndex: %s\nFilter: %s\nMetadata: %s\n",
		newSST.Gen,
		sstable.CreateFileName(dir, newGen, "Data", "db"),
		sstable.CreateFileName(dir, newGen, "Index", "db"),
		sstable.CreateFileName(dir, newGen, "Filter", "db"),
		sstable.CreateFileName(dir, newGen, "Metadata", "db"),
	)
	sstable.WriteTxtToFile(tocPath, tocData)

	return &newSST
}

func (l *LSMTree) sizeTieredCompaction(conf *config.Config, level int) {
	maxSSTablesPerLevel := conf.LSMTree.MaxSSTablesPerLevel[level]

	if len(l.SSTables[level]) < maxSSTablesPerLevel {
		return // ništa za kompakciju
	}

	if level+1 >= len(l.SSTables) {
		l.SSTables = append(l.SSTables, []*sstable.SSTable{}) // dodaj novi nivo ako ne postoji
	}

	// izdvoj prvih N SSTable-ova i spoj ih
	tablesToCompact := l.SSTables[level][:maxSSTablesPerLevel]
	mergedRecords := MergeSSTablesRecords(tablesToCompact)

	newGen := 30 // TODO: Zameni sa FindNextGenerationNumber()
	newSST := BuildSSTableFromRecords(mergedRecords, conf, newGen)

	// Dodaj novi SSTable u sledeći nivo
	l.SSTables[level+1] = append(l.SSTables[level+1], newSST)

	// Obrisi stare SSTable-ove
	for _, t := range tablesToCompact {
		DeleteSSTableFiles(conf, t)
	}

	// Obrisi kompaktovane tabele iz trenutnog nivoa
	l.SSTables[level] = l.SSTables[level][maxSSTablesPerLevel:]

	// Pozovi rekurzivno za sledeći nivo
	l.sizeTieredCompaction(conf, level+1)
}

func (l *LSMTree) leveledCompaction(conf *config.Config, level int) {
	if level+1 >= len(l.SSTables) {
		l.SSTables = append(l.SSTables, []*sstable.SSTable{})
	}

	// Odredi maksimalni limit za nivo, u zavisnosti od podešavanja
	var needCompaction bool
	if conf.LSMTree.UseSizeBasedCompaction {
		// Limit veličine u bajtovima
		maxSizeBytes := int64(conf.LSMTree.BaseLevelSizeMBLimit*1024*1024) * int64(math.Pow(float64(conf.LSMTree.LevelSizeMultiplier), float64(level)))

		var totalSize int64 = 0
		for _, sst := range l.SSTables[level] {
			totalSize += sst.SizeInBytes() // TODO SSTable treba da ima funkciju za veličinu
		}

		needCompaction = totalSize > maxSizeBytes
	} else {
		maxTables := conf.LSMTree.BaseSSTableLimit * int(math.Pow(float64(conf.LSMTree.LevelSizeMultiplier), float64(level)))
		needCompaction = len(l.SSTables[level]) > maxTables
	}

	if !needCompaction {
		return // nema potrebe za kompakcijom na ovom nivou
	}

	// Uzmi prvi SSTable za kompakciju
	sstToCompact := l.SSTables[level][0]

	// Pronađi overlapping SSTable-ove na sledećem nivou
	var overlapping []*sstable.SSTable
	for _, sst := range l.SSTables[level+1] {
		if Overlaps(sstToCompact, sst) {
			overlapping = append(overlapping, sst)
		}
	}

	// Ako ima preklapanja, onda ćemo ih spojiti
	if len(overlapping) != 0 {
		// Merge SSTable-ova i kreiraj novi SSTable
		allToMerge := append([]*sstable.SSTable{sstToCompact}, overlapping...)
		mergedRecords := MergeSSTablesRecords(allToMerge)

		newGen := 30 // TODO: Zameni sa FindNextGenerationNumber()
		newSST := BuildSSTableFromRecords(mergedRecords, conf, newGen)

		l.SSTables[level+1] = append(l.SSTables[level+1], newSST)

		// Briši stare SSTable fajlove
		DeleteSSTableFiles(conf, sstToCompact)
		l.SSTables[level] = l.SSTables[level][1:]

		toDelete := make(map[int]bool)
		for _, o := range overlapping {
			DeleteSSTableFiles(conf, o)
			toDelete[o.Gen] = true
		}

		// Izbaci overlapping SSTable-ove iz sledećeg nivoa
		var updated []*sstable.SSTable
		for _, sst := range l.SSTables[level+1] {
			if !toDelete[sst.Gen] {
				updated = append(updated, sst)
			}
		}
		l.SSTables[level+1] = updated

	} else {
		// Nema overlapping SSTable-ova, samo premesti SSTable
		l.SSTables[level+1] = append(l.SSTables[level+1], sstToCompact)
		l.SSTables[level] = l.SSTables[level][1:] // ukloni sa starog nivoa
		// Nema brisanja fajlova ovde, SSTable i dalje postoji
	}

	// Rekurzivno nastavi za viši nivo
	l.leveledCompaction(conf, level+1)
}

func Overlaps(sst1, sst2 *sstable.SSTable) bool {
	if sst1 == nil || sst2 == nil {
		return false
	}

	// Proveri da li se ključevi preklapaju
	return bytes.Compare(sst1.Summary.Records[0].FirstKey, sst2.Summary.Records[0].LastKey) <= 0 &&
		bytes.Compare(sst2.Summary.Records[0].FirstKey, sst1.Summary.Records[0].LastKey) <= 0
}
