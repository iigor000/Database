package lmstree

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/structures/block_organization"
	"github.com/iigor000/database/structures/cache"
	"github.com/iigor000/database/structures/memtable"
	"github.com/iigor000/database/structures/sstable"
)

// TODO CONFIG
const cacheCapacity = 1000 // Kapacitet keša - DEFINIŠE KORISNIK - CONFIG
const maxLevels = 3        // Maksimalni broj nivoa u LSM stablu - DEFINIŠE KORISNIK - CONFIG
// PARAMETRI ZA KOMPAKCIJU - CONFIG
const size_tiered_compaction = true                    // size tiered ili leveled
const CompactionThreshold = 2                          // Size-Tiered: Koliko SSTable-ova na nivou pokreće kompakciju
const SizeCompactionThreshold int64 = 10 * 1024 * 1024 // 10 MB (primer)

type LSMTree struct {
	Memtable     *memtable.Memtable
	SSTables     [][]*sstable.SSTable
	Cache        *cache.Cache
	BlockManager *block_organization.BlockManager
}

// Kreira novo LSM stablo
func NewLSMTree(cfg *config.BlockConfig) *LSMTree {
	lsm := &LSMTree{
		Cache:        cache.NewCache(cacheCapacity),
		SSTables:     [][]*sstable.SSTable{},
		BlockManager: block_organization.NewBlockManager(cfg),
	}

	lsm.Memtable = RecoverFromWAL()

	return lsm
}

func RecoverFromWAL() *memtable.Memtable {
	// TODO Implementirati funkciju za oporavak iz WAL-a preko Block Manager
	return memtable.NewMemtable(true, 3, 9) // primera radi
}

func WriteWAL(key int, value []byte) bool {
	// TODO Implementirati funkciju za upis u WAL preko Block Manager
	return true
}

// Dodaje ključ i vrednost u Memtable i izvršava flush ako je potrebno
func (l *LSMTree) Put(key int, value []byte) {
	if !WriteWAL(key, value) {
		// Neispravno pisanje u WAL
		return
	}

	// TODO Pisanje u Memtable
	// l.Memtable.Write(key, value)

	// TODO Ako je Memtable pun, izvrši flush na disk
	// if l.Memtable.size >= l.Memtable.capacity {
	l.FlushMemtable()
	//}
}

// Traži ključ u Memtable, Cache i SStables
func (l *LSMTree) Get(key int) ([]byte, bool) {
	// Prvo proveri u Memtable
	if value, exists := l.Memtable.Read(key); exists {
		return value, true
	}

	// Proveri u Cache
	if value, exists := l.Cache.Get(key); exists {
		return value, true
	}

	// Proveri u SSTable-ima
	for _, level := range l.SSTables {
		for _, sst := range level {
			value, exists := sst.Search(key, l.BlockManager)
			if exists {
				// Dodaj u Cache pre vraćanja vrednosti
				l.Cache.Put(key, value)
				return value, true
			}
		}
	}

	return nil, false
}

func (l *LSMTree) Delete(key int) {
	// Prvo izbaci iz Memtable
	l.Memtable.Delete(key)

	// Zatim izbaci iz Cache
	l.Cache.Put(key, nil)

	// Na kraju izbaci iz SSTable-a (ako je potrebno)
	for _, level := range l.SSTables {
		for _, sst := range level {
			sst.Delete(key, l.BlockManager)
		}
	}

	// TODO obrisati odgovarajući WAL segment
}

func (l *LSMTree) FlushMemtable() {
	// TODO SORTIRATI VREDNOSTI PO KLJUČEVIMA U MEMTABLE-U
	// l.Memtable.Sort()
	ID := time.Now().UnixNano()
	newDataPath := fmt.Sprintf("data/level_0/sst_%d_data.db", ID)
	newIndexPath := fmt.Sprintf("data/level_0/sst_%d_data.db", ID)

	// Kreiraj novi SSTable iz Memtable-a
	newSSTable := sstable.NewSSTable(newDataPath, newIndexPath, l.Memtable, l.BlockManager)

	l.SSTables[0] = append(l.SSTables[0], newSSTable)

	l.Compact(0)

	// TODO Resetuj Memtable nakon flusha
	// l.Memtable.Reset()
	// TODO Obrisati odgovarajući WAL segment
}

func (l *LSMTree) Compact(level int) {
	if size_tiered_compaction {
		l.sizeTieredCompaction()
	} else {
		l.leveledCompaction(level)
	}
}

func (l *LSMTree) mergeSSTables(sstables []*sstable.SSTable) []adapter.MemtableEntry {
	entryMap := make(map[int]adapter.MemtableEntry)

	for _, sst := range sstables {
		entries := sst.ReadAll(l.BlockManager)
		for _, entry := range entries {
			// Uvek uzmi najnoviji Timestamp
			if existing, found := entryMap[entry.Key]; !found || entry.Timestamp > existing.Timestamp {
				entryMap[entry.Key] = entry
			}
		}
	}

	// TODO koristiti algoritam kao kod sortiranja MemtableEntry u Memtable strukturi
	// Pretvori mapu u slice i sortiraj
	result := make([]adapter.MemtableEntry, 0, len(entryMap))
	for _, entry := range entryMap {
		if !entry.Tombstone {
			result = append(result, entry)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	return result
}

func (l *LSMTree) leveledCompaction(level int) {
	if level >= len(l.SSTables) || len(l.SSTables[level]) < CompactionThreshold {
		return
	}

	tablesToCompact := l.SSTables[level][:CompactionThreshold]
	mergedEntries := l.mergeSSTables(tablesToCompact)

	if level+1 >= len(l.SSTables) || level+1 > maxLevels {
		l.SSTables = append(l.SSTables, []*sstable.SSTable{})
	}

	ID := time.Now().UnixNano()
	newDataPath := fmt.Sprintf("data/level_%d/sst_%d_data.db", level+1, ID)
	newIndexPath := fmt.Sprintf("data/level_%d/sst_%d_data.db", level+1, ID)

	// Kreiraj novi SSTable iz Memtable-a
	newSSTable := sstable.CreateSSTable(mergedEntries, newDataPath, newIndexPath, l.BlockManager)

	l.SSTables[level+1] = append(l.SSTables[level+1], newSSTable)

	for _, sst := range tablesToCompact {
		os.RemoveAll(sst.GetDataPath())
		os.RemoveAll(sst.GetIndexPath())
	}

	l.SSTables[level] = l.SSTables[level][CompactionThreshold:]
}

func (l *LSMTree) sizeTieredCompaction() {
	// Pretražuju se svi nivoi da bi se pronašli SSTable-ovi koji su dovoljno veliki za kompakciju
	for level, sstables := range l.SSTables {
		if len(sstables) < 2 {
			// Ako nema dovoljno SSTable-ova za kompakciju, prelazi na sledeći nivo
			continue
		}

		// Pronaći grupe SSTable-ova koji mogu biti kompakovani zajedno (grupisani po veličini)
		for i := 0; i < len(sstables)-1; i++ {
			// Ako su sledeći SSTable-ovi dovoljno veliki da budu kompakovani
			if sstables[i].GetSize()+sstables[i+1].GetSize() > SizeCompactionThreshold {
				// Spoji ove SSTable-ove u novi SSTable
				mergedEntries := l.mergeSSTables([]*sstable.SSTable{sstables[i], sstables[i+1]})

				// Kreiraj novi SSTable sa spojenim podacima
				ID := time.Now().UnixNano()
				newDataPath := fmt.Sprintf("data/level_%d/sst_%d_data.db", level, ID)
				newIndexPath := fmt.Sprintf("data/level_%d/sst_%d_data.db", level, ID)

				// Kreiraj novi SSTable
				newSSTable := sstable.CreateSSTable(mergedEntries, newDataPath, newIndexPath, l.BlockManager)

				// Dodaj novi SSTable u trenutni nivo
				l.SSTables[level] = append(l.SSTables[level], newSSTable)

				// Očisti stare SSTable-ove koji su kompakovani
				os.RemoveAll(sstables[i].GetDataPath())
				os.RemoveAll(sstables[i].GetIndexPath())
				os.RemoveAll(sstables[i+1].GetDataPath())
				os.RemoveAll(sstables[i+1].GetIndexPath())

				// Ukloni staru grupu SSTable-ova iz liste
				l.SSTables[level] = append(l.SSTables[level][:i], l.SSTables[level][i+2:]...)

				break
			}
		}
	}
}
