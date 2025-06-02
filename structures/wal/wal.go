package main

import (
	"hash/crc32"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

const (
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func MakeEntry(timestamp int64, tombstone bool, key []byte, value []byte) []byte {
	keySize := len(key)
	valueSize := len(value)

	entry := make([]byte, CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE+KEY_SIZE_SIZE+VALUE_SIZE_SIZE+keySize+valueSize)

	// Fill in the entry
	copy(entry[TIMESTAMP_START:], int64ToBytes(timestamp))
	entry[TOMBSTONE_START] = boolToByte(tombstone)
	copy(entry[KEY_SIZE_START:], int64ToBytes(int64(keySize)))
	copy(entry[VALUE_SIZE_START:], int64ToBytes(int64(valueSize)))
	copy(entry[KEY_START:], key)
	copy(entry[KEY_START+keySize:], value)

	// Calculate and set the CRC
	crc := CRC32(entry[CRC_START+TIMESTAMP_SIZE:])
	copy(entry[CRC_START:], int32ToBytes(crc))

	return entry
}

// int32ToBytes converts a uint32 to a byte slice in big-endian order.
func int32ToBytes(i uint32) []byte {
	b := make([]byte, 4)
	for idx := 3; idx >= 0; idx-- {
		b[idx] = byte(i)
		i >>= 8
	}
	return b
}

// int64ToBytes converts an int64 to a byte slice in big-endian order.
func int64ToBytes(i int64) []byte {
	b := make([]byte, 8)
	for idx := 7; idx >= 0; idx-- {
		b[idx] = byte(i)
		i >>= 8
	}
	return b
}

// boolToByte converts a bool to a byte (1 for true, 0 for false).
func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}
