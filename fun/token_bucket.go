package fun

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/iigor000/database/util"
)

// Pravljenje novog baketa za korisnika
func CreateBucket(db *Database) error {
	// Za roota bypassujemo sve
	if db.username == "root" {
		return nil
	}

	// Ako korisnik vec postoji, ne pravimo baket
	_, found, err := db.get(util.TokenBucketPrefix + db.username)
	if err != nil {
		return fmt.Errorf("error getting token bucket: %w", err)
	}
	if found {
		return nil
	}

	// Inicijalizujemo novi baket
	bucket := map[string]interface{}{
		"tokens":    db.config.TokenBucket.StartTokens,
		"timestamp": int64(time.Now().Unix()),
	}
	data, err := json.Marshal(bucket)
	if err != nil {
		return fmt.Errorf("failed to marshal token bucket: %w", err)
	}

	// Stavljamo baket u bazu
	err = db.put(util.TokenBucketPrefix+db.username, data)
	if err != nil {
		return fmt.Errorf("failed to store token bucket: %w", err)
	}

	return nil
}

// Proverava da li korisnik ima validan token
func CheckBucket(db *Database) (bool, error) {
	if db.username == "root" {
		return true, nil
	}

	// Citamo broj tokena korisnika
	value, found, err := db.get(util.TokenBucketPrefix + db.username)
	if err != nil {
		return false, fmt.Errorf("error getting token bucket: %w", err)
	}
	if !found {
		return false, errors.New("token bucket not found for user: " + db.username)
	}

	// Pretvaramo u citljive podatke
	var bucket map[string]interface{}
	if err := json.Unmarshal(value, &bucket); err != nil {
		return false, fmt.Errorf("failed to unmarshal token bucket: %w", err)
	}

	tokens, ok := bucket["tokens"].(float64)
	if !ok {
		return false, errors.New("invalid token count in bucket")
	}

	currentTime := time.Now().Unix()
	bucketTime := int64(bucket["timestamp"].(float64))

	// Ako ima vise od 0 tokena, smanjujemo broj tokena
	if tokens > 0 {
		bucket["tokens"] = tokens - 1
		newData, err := json.Marshal(bucket)
		if err != nil {
			return false, err
		}

		// Pisemo novi broj u databazu
		if err := db.put(util.TokenBucketPrefix+db.username, newData); err != nil {
			return false, err
		}
		return true, nil
	}

	// Ako nema tokena, gledamo da li je proslo vreme, pa ako jeste dopunjavamo ih
	if currentTime-bucketTime > int64(db.config.TokenBucket.RefillIntervalS) {
		bucket["tokens"] = db.config.TokenBucket.StartTokens
		bucket["timestamp"] = currentTime
		newData, err := json.Marshal(bucket)
		if err != nil {
			return false, err
		}

		if err := db.put(util.TokenBucketPrefix+db.username, newData); err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}
