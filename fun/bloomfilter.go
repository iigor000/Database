package fun

import (
	"errors"

	"github.com/iigor000/database/structures/bloomfilter"
	"github.com/iigor000/database/util"
)

func (db *Database) NewBloomFilter(key string, u int, p float64) error {
	// Proveravamo da li po token bucketu korisnik moze da unese podatke
	allow, err := CheckBucket(db)
	if err != nil {
		return err
	}
	if !allow {
		return errors.New("user has reached the rate limit") // Korisnik ne moze da unese podatke
	}

	// Kreiramo BloomFilter i zapisujemo ga u SSTable
	bf := bloomfilter.MakeBloomFilter(u, p)

	key = util.BloomFilterPrefix + key

	err = db.put(key, bf.Serialize())
	if err != nil {
		return err
	}

	return nil
}

func (db *Database) DeleteBloomFilter(key string) error {
	// Proveravamo da li po token bucketu korisnik moze da unese podatke
	allow, err := CheckBucket(db)
	if err != nil {
		return err
	}
	if !allow {
		return errors.New("user has reached the rate limit") // Korisnik ne moze da unese podatke
	}

	key = util.BloomFilterPrefix + key

	// Brisemo BloomFilter iz SSTable
	return db.Delete(key)
}

func (db *Database) AddToBloomFilter(key string, value []byte) error {
	// Proveravamo da li po token bucketu korisnik moze da unese podatke
	allow, err := CheckBucket(db)
	if err != nil {
		return err
	}
	if !allow {
		return errors.New("user has reached the rate limit") // Korisnik ne moze da unese podatke
	}

	key = util.BloomFilterPrefix + key

	// Dodajemo vrednost u BloomFilter
	bloomFilterData, found, err := db.get(key)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("bloom filter not found")
	}

	bf := bloomfilter.Deserialize(bloomFilterData)
	bf[0].Add(value)

	return db.put(key, bf[0].Serialize())
}

func (db *Database) CheckInBloomFilter(key string, value []byte) (bool, error) {
	// Proveravamo da li po token bucketu korisnik moze da unese podatke
	allow, err := CheckBucket(db)
	if err != nil {
		return false, err
	}
	if !allow {
		return false, errors.New("user has reached the rate limit") // Korisnik ne moze da unese podatke
	}

	key = util.BloomFilterPrefix + key

	// Proveravamo da li je vrednost u BloomFilteru
	bloomFilterData, found, err := db.get(key)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil // Bloom filter ne postoji
	}

	bf := bloomfilter.Deserialize(bloomFilterData)
	return bf[0].Read(value), nil
}
