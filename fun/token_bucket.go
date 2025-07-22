package fun

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/iigor000/database/util"
)

func CreateBucket(db *Database) error {
	bucket := map[string]interface{}{
		"tokens":    db.config.TokenBucket.StartTokens,
		"timestamp": int64(time.Now().Unix()),
	}
	data, err := json.Marshal(bucket)
	if err != nil {
		return err
	}
	err = db.put(util.TokenBucketPrefix+db.username, data)
	if err != nil {
		return err
	}
	return nil
}

func CheckBucket(db *Database) (bool, error) {
	if db.username == "root" {
		return true, nil
	}

	value, found, err := db.get(util.TokenBucketPrefix + db.username)

	if err != nil {
		return false, err
	}
	if !found {
		return false, errors.New("token bucket not found for user: " + db.username)
	}

	var bucket map[string]interface{}
	err = json.Unmarshal(value, &bucket)
	if err != nil {
		return false, err
	}

	tokens, ok := bucket["tokens"].(float64)
	if !ok {
		return false, errors.New("tokens not found or invalid type in bucket for user: " + db.username)
	}
	if tokens > 0 {
		// User can make a request, decrement the token count
		bucket["tokens"] = tokens - 1
		data, err := json.Marshal(bucket)
		if err != nil {
			return false, err
		}
		err = db.put(util.TokenBucketPrefix+db.username, data)
		if err != nil {
			return false, err
		}
		// Allow the request
		return true, nil
	}
	timestamp, ok := bucket["timestamp"]
	if !ok {
		return false, errors.New("timestamp not found in bucket for user: " + db.username)
	}
	if time.Now().Unix()-int64(timestamp.(float64)) > int64(db.config.TokenBucket.RefillIntervalS) {
		// Reset tokens if more than 60 seconds have passed
		bucket["tokens"] = db.config.TokenBucket.StartTokens
		bucket["timestamp"] = time.Now().Unix()
		data, err := json.Marshal(bucket)
		if err != nil {
			return false, err
		}
		err = db.put(util.TokenBucketPrefix+db.username, data)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}
