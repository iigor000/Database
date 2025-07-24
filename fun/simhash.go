package fun

import (
	"errors"

	"github.com/iigor000/database/structures/simhash"
	"github.com/iigor000/database/util"
)

func (db *Database) AddSHFingerprint(key string, text string) error {
	// Proveravamo da li po token bucketu korisnik moze da unese podatke
	allow, err := CheckBucket(db)
	if err != nil {
		return err
	}
	if !allow {
		return errors.New("user has reached the rate limit") // Korisnik ne moze da unese podatke
	}

	key = util.SimHashPrefix + key

	fingerprint := simhash.SimHash(text)
	return db.put(key, fingerprint)
}

func (db *Database) DeleteSHFingerprint(key string) error {
	// Proveravamo da li po token bucketu korisnik moze da unese podatke
	allow, err := CheckBucket(db)
	if err != nil {
		return err
	}
	if !allow {
		return errors.New("user has reached the rate limit") // Korisnik ne moze da unese podatke
	}

	key = util.SimHashPrefix + key

	// Brisemo SimHash fingerprint iz SSTable
	return db.Delete(key)
}

func (db *Database) GetHemmingDistance(key1 string, key2 string) (int, error) {
	// Proveravamo da li po token bucketu korisnik moze da unese podatke
	allow, err := CheckBucket(db)
	if err != nil {
		return 0, err
	}
	if !allow {
		return 0, errors.New("user has reached the rate limit") // Korisnik ne moze da unese podatke
	}

	key1 = util.SimHashPrefix + key1
	key2 = util.SimHashPrefix + key2

	fingerprint1, found1, err := db.get(key1)
	if err != nil {
		return 0, err
	}
	if !found1 {
		return 0, errors.New("SimHash fingerprint not found for key: " + key1)
	}

	fingerprint2, found2, err := db.get(key2)
	if err != nil {
		return 0, err
	}
	if !found2 {
		return 0, errors.New("SimHash fingerprint not found for key: " + key2)
	}

	distance := simhash.CompareHashes(fingerprint1, fingerprint2)
	return distance, nil
}
