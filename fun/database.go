package fun

import (
	"time"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/adapter"
	"github.com/iigor000/database/structures/cache"
	"github.com/iigor000/database/structures/memtable"
)

//TODO: Dodati wal i kompresiju kad budu zavrseni

type Database struct {
	//wal
	//compression
	memtables *memtable.Memtables
	config    *config.Config
	cache     *cache.Cache
}

func NewDatabase(config config.Config) (*Database, error) {
	memtables := memtable.NewMemtables(config)

	cache, err := cache.NewCache(config)
	if err != nil {
		return nil, err
	}

	return &Database{
		memtables: memtables,
		config:    &config,
		cache:     cache,
	}, nil
}

func (db *Database) put(key string, value []byte) error {
	record := adapter.MemtableEntry{
		Key:       []byte(key),
		Value:     value,
		Timestamp: int64(time.Now().Unix()), // Pretpostavljamo da config sadrži trenutni timestamp
		Tombstone: false,                    // Postavljamo Tombstone na false prilikom dodavanja
	}

	return nil
}

func (db *Database) get(key string) ([]byte, bool) {
	return nil, false
}

func (db *Database) delete(key string) error {

	return nil
}
