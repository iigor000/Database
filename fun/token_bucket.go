package fun

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/iigor000/database/util"
)

func CreateBucket(db *Database) error {
	if db.username == "root" {
		return nil
	}

	_, found, err := db.get(util.TokenBucketPrefix + db.username)
	if err != nil {
		return fmt.Errorf("error getting token bucket: %w", err)
	}
	if found {
		return nil
	}

	bucket := map[string]interface{}{
		"tokens":    db.config.TokenBucket.StartTokens,
		"timestamp": int64(time.Now().Unix()),
	}
	data, err := json.Marshal(bucket)
	if err != nil {
		return fmt.Errorf("failed to marshal token bucket: %w", err)
	}

	// Use the unexported put to bypass token checks
	err = db.put(util.TokenBucketPrefix+db.username, data)
	if err != nil {
		return fmt.Errorf("failed to store token bucket: %w", err)
	}

	fmt.Printf("Created token bucket for user: %s\n", db.username)
	return nil
}

func CheckBucket(db *Database) (bool, error) {
	if db.username == "root" {
		return true, nil
	}

	// Use unexported get to bypass token checks
	value, found, err := db.get(util.TokenBucketPrefix + db.username)
	if err != nil {
		return false, fmt.Errorf("error getting token bucket: %w", err)
	}
	if !found {
		return false, errors.New("token bucket not found for user: " + db.username)
	}

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

	if tokens > 0 {
		// User has tokens, decrement and update
		bucket["tokens"] = tokens - 1
		newData, err := json.Marshal(bucket)
		if err != nil {
			return false, err
		}

		// Use unexported put to update
		if err := db.put(util.TokenBucketPrefix+db.username, newData); err != nil {
			return false, err
		}
		return true, nil
	}

	// Check if we should refill tokens
	if currentTime-bucketTime > int64(db.config.TokenBucket.RefillIntervalS) {
		bucket["tokens"] = db.config.TokenBucket.StartTokens
		bucket["timestamp"] = currentTime
		newData, err := json.Marshal(bucket)
		if err != nil {
			return false, err
		}

		// Use unexported put to update
		if err := db.put(util.TokenBucketPrefix+db.username, newData); err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}
