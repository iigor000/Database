package fun

import (
	"time"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/cache"
	"github.com/iigor000/database/structures/memtable"
	"github.com/iigor000/database/structures/sstable"
)

//TODO: Dodati wal i kompresiju kad budu zavrseni

type Database struct {
	//wal
	memtables *memtable.Memtables
	config    *config.Config
	cache     *cache.Cache
}

func NewDatabase(config *config.Config) (*Database, error) {
	memtables := memtable.NewMemtables(config)

	cache := cache.NewCache(config)

	return &Database{
		memtables: memtables,
		config:    config,
		cache:     cache,
	}, nil
}

func (db *Database) Put(key string, value string) error {

	// TODO: Staviti write ahead log zapis

	// TODO: Proveriti jel treba tu biti kompresija

	shouldFlush := db.memtables.Update([]byte(key), []byte(value), int64(time.Now().Unix()), false)

	if shouldFlush {
		// Flush Memtable na disk

		sstable.FlushSSTable(db.config, *db.memtables.Memtables[db.memtables.NumberOfMemtables-1], db.memtables.GenToFlush)

		recordsToCache := db.memtables.Memtables[db.memtables.NumberOfMemtables-1].GetAllEntries()

		// Resetujemo redosled Memtable-a
		for j := 0; j < db.memtables.NumberOfMemtables-1; j++ {
			db.memtables.Memtables[j] = db.memtables.Memtables[j+1]
		}

		// Dodajemo novi Memtable na kraj
		db.memtables.Memtables[db.memtables.NumberOfMemtables-1] = memtable.NewMemtable(db.config.Memtable.Structure == "skiplist", db.config.Skiplist.MaxHeight, db.config.Memtable.NumberOfEntries)

		//TODO: Povecati generaciju za flush
		db.memtables.GenToFlush++

		//TODO: Dodati LSM stablo

		//TODO: Zapisati u wal da je flush uradjen

		for _, record := range recordsToCache {
			// Dodajemo u cache
			if _, found := db.cache.Get(string(record.Key)); found {
				db.cache.Put(record)
			}
		}

	}

	return nil
}

func (db *Database) Get(key string) ([]byte, bool, error) {
	keyByte := []byte(key)

	// Proveravamo da li je u Memtable-u
	entry, found := db.memtables.Search(keyByte)
	if found {
		if !entry.Tombstone {
			return entry.Value, true, nil
		}
		return nil, false, nil
	}

	// Proveravamo da li je u cache-u
	entry, found = db.cache.Get(key)
	if found {
		if !entry.Tombstone {
			return entry.Value, true, nil
		}
		return nil, false, nil
	}

	// TODO: Uzeti iz lsm stabla

	// Ako se nalazi u LSM stablu, stavljamo ga u cache
	if entry != nil {
		db.cache.Put(*entry)
		if !entry.Tombstone {
			return entry.Value, true, nil
		}
	}

	return nil, false, nil
}

func (db *Database) Delete(key string) error {

	// TODO: Napisati u wal da se brise entry

	// Brisanje iz memtable-a
	found := db.memtables.Delete([]byte(key))
	if !found {
		err := db.Put(key, "")
		if err != nil {
			return err
		}

		db.memtables.Delete([]byte(key))
	}

	return nil
}
