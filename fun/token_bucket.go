package fun

import (
	"encoding/json"
	"errors"
	"time"
)

func CreateBucket(db *Database) error {
	bucket := map[string]interface{}{
		"tokens":    5,
		"timestamp": int64(time.Now().Unix()),
	}
	data, err := json.Marshal(bucket)
	if err != nil {
		return err
	}
	err = db.put(db.username, data)
	if err != nil {
		return err
	}
	return nil
}

func CheckBucket(db *Database) (bool, error) {
	value, found, err := db.get(db.username)

	if err != nil {
		return false, err
	}
	if !found {
		return false, errors.New("token bucket not foun for user: " + db.username)
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
		err = db.put(db.username, data)
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
	if time.Now().Unix()-int64(timestamp.(float64)) > 120 {
		// Reset tokens if more than 60 seconds have passed
		bucket["tokens"] = 5
		bucket["timestamp"] = time.Now().Unix()
		data, err := json.Marshal(bucket)
		if err != nil {
			return false, err
		}
		err = db.put(db.username, data)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}
