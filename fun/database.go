package fun

import (
	"errors"
	"fmt"
	"time"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/structures/adapter"

	"github.com/iigor000/database/structures/block_organization"
	"github.com/iigor000/database/structures/cache"
	"github.com/iigor000/database/structures/compression"
	writeaheadlog "github.com/iigor000/database/structures/writeAheadLog"

	"github.com/iigor000/database/structures/lsmtree"
	"github.com/iigor000/database/structures/memtable"
	"github.com/iigor000/database/structures/sstable"
	"github.com/iigor000/database/util"
)

//TODO: Dodati (wal) i kompresiju kad budu zavrseni

type Database struct {
	wal               *writeaheadlog.WAL
	compression       *compression.Dictionary
	memtables         *memtable.Memtables
	config            *config.Config
	cache             *cache.Cache
	username          string
	lastFlushedGen    int // poslednja generacija koja je flush-ovana na disk
	CacheBlockManager *block_organization.CachedBlockManager
}

func NewDatabase(config *config.Config, username string) (*Database, error) {
	// Prvo kreiramo CachedBlockManager
	bm := block_organization.NewBlockManager(config)
	bc := block_organization.NewBlockCache(config)
	cbm := &block_organization.CachedBlockManager{
		BM: bm,
		C:  bc,
	}

	wal, err := writeaheadlog.SetOffWAL(config, cbm)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize write-ahead log: %w", err)
	}

	//ucitaj wal u memtable
	memtables := memtable.NewMemtables(config)
	records, err := wal.ReadRecords()
	if err != nil {
		return nil, fmt.Errorf("failed to read records from write-ahead log: %w", err)
	}
	for _, record := range records {
		if record.Tombstone {
			// Ako je tombstone, brisemo kljuc, ne treba nam u memtable
			memtables.Delete(record.Key)
		} else {
			// Ako nije tombstone, dodajemo kljuc i vrednost u memtable
			memtables.Update(record.Key, record.Value, record.Timestamp, false)
		}
	}
	//?? TODO: Treba da se ucita BloomFilter i Summary iz SSTable-a
	cache := cache.NewCache(config)
	dict, err := compression.Read(config.Compression.DictionaryDir, cbm)
	if err != nil {
		dict = compression.NewDictionary() // Ako nije uspelo da se ucita, kreiramo novi
	}

	return &Database{
		wal:               wal,
		memtables:         memtables,
		config:            config,
		cache:             cache,
		username:          username,
		compression:       dict,
		CacheBlockManager: cbm,
	}, nil
}

func (db *Database) calculateLWM() int {
	return db.lastFlushedGen
	// ako imamo flushovane generacije npr 1 i 2, onda je lwm 1
	// ostaje taj nivo dok se flush skroz ne zavrsi
}

func (db *Database) Put(key string, value []byte) error {
	// Proveravamo da li po token bucketu korisnik moze da unese podatke
	allow, err := CheckBucket(db)
	if err != nil {
		return err
	}
	if !allow {
		return errors.New("user has reached the rate limit") // Korisnik ne moze da unese podatke
	}

	// Proveravamo da li je kljuc rezervisan
	if util.CheckKeyReserved(key) {
		return errors.New("key is reserved: " + key)
	}

	return db.put(key, value)
}

func (db *Database) put(key string, value []byte) error {

	if err := db.wal.Append([]byte(key), value, false); err != nil {
		return fmt.Errorf("failed to append to write-ahead log: %w", err)
	}

	if db.compression == nil {
		db.compression = compression.NewDictionary()
	}
	db.compression.Add([]byte(key))
	shouldFlush := db.memtables.Update([]byte(key), []byte(value), int64(time.Now().Unix()), false)

	if shouldFlush {
		// Flush Memtable na disk
		println("Flushing Memtable to disk...")
		println("Gen to Flush:", db.memtables.GenToFlush)
		sstable.FlushSSTable(db.config, *db.memtables.Memtables[0], 1, db.memtables.GenToFlush, db.compression, db.CacheBlockManager)

		db.lastFlushedGen = db.memtables.GenToFlush // azuriramo poslednju flushovanu generaciju
		if err := db.wal.RemoveSegmentsUpTo(db.calculateLWM()); err != nil {
			return fmt.Errorf("failed to remove write-ahead log segments up to lwm: %w", err)
		}

		// Proverava uslov za kompakciju i vrši kompakciju ako je potrebno (počinje proveru od prvog nivoa)
		lsmtree.Compact(db.config, db.compression, db.CacheBlockManager)

		recordsToCache := db.memtables.Memtables[0].GetAllEntries()

		// Resetujemo redosled Memtable-a
		for j := 0; j < db.memtables.NumberOfMemtables-1; j++ {
			db.memtables.Memtables[j] = db.memtables.Memtables[j+1]
		}

		// Dodajemo novi Memtable na kraj
		db.memtables.Memtables[db.memtables.NumberOfMemtables-1] = memtable.NewMemtable(db.config)

		// Ako se desi kompakcija, može se promeniti broj sledeće generacije SSTable-a
		db.memtables.GenToFlush = lsmtree.GetNextSSTableGeneration(db.config, 1)

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
	// Proveravamp da li po token bucketu korisnik moze da unese podatke
	allow, err := CheckBucket(db)
	if err != nil {
		return nil, false, err
	}
	if !allow {
		return nil, false, errors.New("user has reached the rate limit") // Korisnik ne moze da unese podatke
	}

	// Proveravamo da li je kljuc rezervisan
	if util.CheckKeyReserved(key) {
		return nil, false, errors.New("key is reserved: " + key)
	}

	return db.get(key)
}

func (db *Database) get(key string) ([]byte, bool, error) {

	keyByte := []byte(key)

	// Proveravamo da li je u Memtable-u
	entry, found := db.memtables.Search(keyByte)
	if found {
		if !entry.Tombstone {
			println("Found in Memtable:", key, "Value:", string(entry.Value))
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

	record, err := lsmtree.Get(db.config, keyByte, db.compression, db.CacheBlockManager)
	if err != nil {
		return nil, false, err
	} else {
		if record == nil {
			return nil, false, nil // Nije pronađen ključ
		}
		entry = &adapter.MemtableEntry{
			Key:       record.Key,
			Value:     record.Value,
			Timestamp: record.Timestamp,
			Tombstone: record.Tombstone,
		}
	}

	// Ako se nalazi u LSM stablu, stavljamo ga u cache
	db.cache.Put(*entry)
	if !entry.Tombstone {
		return entry.Value, true, nil
	}

	return nil, false, nil
}

func (db *Database) Delete(key string) error {
	// Proveravamp da li po token bucketu korisnik moze da unese podatke
	allow, err := CheckBucket(db)
	if err != nil {
		return err
	}
	if !allow {
		return errors.New("user has reached the rate limit") // Korisnik ne moze da unese podatke
	}

	// Proveravamo da li je kljuc rezervisan
	if util.CheckKeyReserved(key) {
		return errors.New("key is reserved: " + key)
	}

	return db.delete(key)
}

func (db *Database) delete(key string) error {
	// TODO: Napisati u wal da se brise entry
	if err := db.wal.Append([]byte(key), nil, true); err != nil {
		return fmt.Errorf("failed to write to write-ahead log: %w", err)
	}

	// Brisanje iz memtable-a
	found := db.memtables.Delete([]byte(key))
	if !found {
		err := db.Put(key, []byte{})
		if err != nil {
			return err
		}

		db.memtables.Delete([]byte(key))
	}

	return nil
}
