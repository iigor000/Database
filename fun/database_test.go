package fun

import (
	"fmt"
	"testing"

	"github.com/iigor000/database/config"
)

func TestMakeDatabase(t *testing.T) {
	// Test creating a new database with a valid configuration
	config, err := config.LoadConfigFile("config/config.json")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	username := "testuser"
	db, err := NewDatabase(config, username)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	if db.username != username {
		t.Errorf("Expected username %s, got %s", username, db.username)
	}
}

func TestDatabase_PutGet(t *testing.T) {
	// Test inserting a key-value pair into the database
	config, err := config.LoadConfigFile("config/config.json")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	username := "testuser"
	db, err := NewDatabase(config, username)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	CreateBucket(db)

	key := "testkey"
	value := []byte("testvalue")

	err = db.Put(key, value)
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	storedValue, found, err := db.get(key)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	if !found || string(storedValue) != string(value) {
		t.Errorf("Expected value %s, got %s", value, storedValue)
	}
}

func TestDatabase_Delete(t *testing.T) {
	// Test deleting a key from the database
	config, err := config.LoadConfigFile("config/config.json")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	username := "testuser"
	db, err := NewDatabase(config, username)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	CreateBucket(db)

	key := "testkey"
	value := []byte("testvalue")

	err = db.Put(key, value)
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	err = db.Delete(key)
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	_, found, err := db.get(key)
	if err != nil {
		t.Fatalf("Failed to get value after delete: %v", err)
	}
	if found {
		t.Errorf("Expected key %s to be deleted, but it was found", key)
	}
}

func TestDatabase_PutMany(t *testing.T) {
	// Test inserting multiple key-value pairs into the database
	config, err := config.LoadConfigFile("../config/config.json")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	username := "root"
	db, err := NewDatabase(config, username)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	CreateBucket(db)

	entries := make(map[string][]byte)

	for i := 0; i < 500; i++ {
		entries[fmt.Sprintf("key%d", i)] = []byte(fmt.Sprintf("value%d", i))
	}

	for k, v := range entries {
		err = db.Put(k, v)
		if err != nil {
			t.Fatalf("Failed to put value for key %s: %v", k, err)
		}
	}

	for k, v := range entries {
		storedValue, found, err := db.get(k)
		if err != nil {
			t.Fatalf("Failed to get value for key %s: %v", k, err)
		}
		//println("Key:", k, "Value:", string(storedValue))
		if !found || string(storedValue) != string(v) {
			t.Errorf("Expected value for key %s to be %s, got %s", k, v, storedValue)
		}
	}
}
