package fun

import (
	"errors"

	"github.com/iigor000/database/structures/cms"
	"github.com/iigor000/database/util"
)

func (db *Database) CreateCMS(key string, epsilon float64, delta float64) error {
	// Proveravamo da li po token bucketu korisnik moze da unese podatke
	allow, err := CheckBucket(db)
	if err != nil {
		return err
	}
	if !allow {
		return errors.New("user has reached the rate limit") // Korisnik ne moze da unese podatke
	}

	cms := cms.MakeCountMinSketch(epsilon, delta)

	key = util.CMSPrefix + key

	return db.put(key, cms.Serialize())
}

func (db *Database) DeleteCMS(key string) error {
	// Proveravamo da li po token bucketu korisnik moze da unese podatke
	allow, err := CheckBucket(db)
	if err != nil {
		return err
	}
	if !allow {
		return errors.New("user has reached the rate limit") // Korisnik ne moze da unese podatke
	}

	key = util.CMSPrefix + key

	// Brisemo CountMinSketch iz SSTable
	return db.Delete(key)
}

func (db *Database) AddToCMS(key string, value []byte) error {
	// Proveravamo da li po token bucketu korisnik moze da unese podatke
	allow, err := CheckBucket(db)
	if err != nil {
		return err
	}
	if !allow {
		return errors.New("user has reached the rate limit") // Korisnik ne moze da unese podatke
	}

	key = util.CMSPrefix + key

	cmsData, found, err := db.get(key)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("CountMinSketch not found for key: " + key)
	}

	cms := cms.Deserialize(cmsData)

	cms[0].Add(value)

	return db.put(key, cms[0].Serialize())
}

func (db *Database) CheckInCMS(key string, value []byte) (uint64, error) {
	// Proveravamo da li po token bucketu korisnik moze da unese podatke
	allow, err := CheckBucket(db)
	if err != nil {
		return 0, err
	}
	if !allow {
		return 0, errors.New("user has reached the rate limit") // Korisnik ne moze da unese podatke
	}

	key = util.CMSPrefix + key

	cmsData, found, err := db.get(key)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, errors.New("CountMinSketch not found for key: " + key)
	}

	cms := cms.Deserialize(cmsData)

	return cms[0].Read(value), nil
}
