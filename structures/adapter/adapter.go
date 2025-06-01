package adapter

// Pomocne struktrue i interfejsi za rad sa memtable strukturom zbog opcione implementacije binarnim stablom ili skip listom
// MemtableEntry - struktura koja predstavlja jedan zapis u memtable strukturi
// MemtableStructure - interfejs koji definiše osnovne operacije nad memtable strukturom

type MemtableEntry struct {
	Key       []byte
	Value     []byte
	Timestamp int64
	Tombstone bool
}

type MemtableStructure interface {
	Update(key []byte, value []byte, timestamp int64, tombstone bool)
	Search(key []byte) (*MemtableEntry, bool)
	Delete(key []byte)
	Clear()
}
