package adapter

// Pomocne struktrue i interfejsi za rad sa memtable strukturom zbog opcione implementacije binarnim stablom ili skip listom
// MemtableEntry - struktura koja predstavlja jedan zapis u memtable strukturi
// MemtableStructure - interfejs koji definiše osnovne operacije nad memtable strukturom

type MemtableEntry struct {
	Key       int
	Value     []byte
	Timestamp int64
	Tombstone bool
}

type MemtableStructure interface {
	Create(key int, value []byte, timestamp int64, tombstone bool)
	Read(key int) (*MemtableEntry, bool)
	Update(key int, value []byte)
	Delete(key int)
	Clear()
}
