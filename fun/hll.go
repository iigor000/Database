package fun

import (
	"errors"

	"github.com/iigor000/database/structures/hyperloglog"
	"github.com/iigor000/database/util"
)

func (db *Database) CreateHLL(key string, precision int) error {
	// Proveravamo da li po token bucketu korisnik moze da unese podatke
	allow, err := CheckBucket(db)
	if err != nil {
		return err
	}
	if !allow {
		return errors.New("user has reached the rate limit") // Korisnik ne moze da unese podatke
	}

	key = util.HLLPrefix + key

	hll, err := hyperloglog.MakeHyperLogLog(precision)
	if err != nil {
		return err
	}

	return db.put(key, hll.Serialize())
}

func (db *Database) DeleteHLL(key string) error {
	// Proveravamo da li po token bucketu korisnik moze da unese podatke
	allow, err := CheckBucket(db)
	if err != nil {
		return err
	}
	if !allow {
		return errors.New("user has reached the rate limit") // Korisnik ne moze da unese podatke
	}

	key = util.HLLPrefix + key

	// Brisemo HyperLogLog iz SSTable
	return db.Delete(key)
}

func (db *Database) AddToHLL(key string, value []byte) error {
	// Proveravamo da li po token bucketu korisnik moze da unese podatke
	allow, err := CheckBucket(db)
	if err != nil {
		return err
	}
	if !allow {
		return errors.New("user has reached the rate limit") // Korisnik ne moze da unese podatke
	}

	key = util.HLLPrefix + key

	hllData, found, err := db.get(key)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("HyperLogLog not found for key: " + key)
	}

	hll := hyperloglog.Deserialize(hllData)

	hll.Add(value)

	return db.put(key, hll.Serialize())
}

func (db *Database) EstimateHLL(key string) (float64, error) {
	// Proveravamo da li po token bucketu korisnik moze da unese podatke
	allow, err := CheckBucket(db)
	if err != nil {
		return 0, err
	}
	if !allow {
		return 0, errors.New("user has reached the rate limit") // Korisnik ne moze da unese podatke
	}

	key = util.HLLPrefix + key

	hllData, found, err := db.get(key)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, errors.New("HyperLogLog not found for key: " + key)
	}

	hll := hyperloglog.Deserialize(hllData)

	return hll.Estimate(), nil
}
