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

		//recordsToCache := db.memtables.Memtables[db.memtables.NumberOfMemtables-1].GetAllEntries()

		// Resetujemo redosled Memtable-a
		for j := 0; j < db.memtables.NumberOfMemtables-1; j++ {
			db.memtables.Memtables[j] = db.memtables.Memtables[j+1]
		}

		// Dodajemo novi Memtable na kraj
		db.memtables.Memtables[db.memtables.NumberOfMemtables-1] = memtable.NewMemtable(db.config, db.config.Memtable.NumberOfEntries)

		//TODO: Povecati generaciju za flush
		db.memtables.GenToFlush++

		//TODO: Dodati LSM stablo

		//TODO: Zapisati u wal da je flush uradjen

		// for _, record := range recordsToCache {
		// 	// Dodajemo u cache
		// 	db.cache.Put(string(record.Key), record.Value)
		// }

	}

	return nil
}

func (db *Database) Get(key string) ([]byte, bool) {
	keyByte := []byte(key)

	entry, found := db.memtables.Search(keyByte)
	if found {
		if !entry.Tombstone {
			return entry.Value, true
		} else {
			return nil, false
		}
	}

	// TODO: Uzeti iz cacha ako je tu, cache bi trebao da sadrzi i tombstone i timestamp
	//entry := db.cache.Get(key)

	// TODO: Uzeti iz lsm stabla

	// TODO: Ako ga nadjemo na kraju, stavljamo ga u cache ako nije vec u njemu

	return nil, false
}

func (db *Database) Delete(key string) error {

	// TODO: Napisati u wal da se brise entry

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
