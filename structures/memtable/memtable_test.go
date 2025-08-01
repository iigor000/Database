package memtable

import (
	"fmt"
	"testing"

	"github.com/iigor000/database/config"
)

func TestMemtableCRUD(t *testing.T) {
	m := NewMemtable(&config.Config{
		Memtable: config.MemtableConfig{
			NumberOfEntries: 5,
			Structure:       "skiplist",
		},
		Skiplist: config.SkiplistConfig{
			MaxHeight: 3,
		},
		BTree: config.BTreeConfig{
			MinSize: 16,
		},
	})
	// Test initial state
	if len(m.Keys) != 0 {
		t.Error("Expected empty Memtable at initialization")
	}
	// Test Update and Search
	fmt.Println("Adding entries to Memtable:")
	m.Update([]byte("1"), []byte("one"), 1, false)
	//fmt.Println("Added key 1 with value 'one'")
	m.Update([]byte("4"), []byte("four"), 4, false)
	m.Update([]byte("7"), []byte("seven"), 7, false)
	m.Update([]byte("10"), []byte("ten"), 10, false)
	m.Update([]byte("12"), []byte("twelve"), 12, false)
	fmt.Println("Added keys 1, 4, 7, 10, 12")
	fmt.Println("Updating keys 1, 4, 7, 12 with new values:")
	m.Update([]byte("1"), []byte("newone"), 13, false)
	m.Update([]byte("4"), []byte("newfour"), 14, false)
	m.Update([]byte("7"), []byte("newseven"), 15, false)
	m.Update([]byte("12"), []byte("newtwelve"), 16, false)
	fmt.Println("Updated keys 1, 4, 7, 12 with new values")

	entry, found := m.Search([]byte("1"))
	value := entry.Value
	if !found || string(value) != "newone" {
		t.Error("Expected newone")
	}
	entry, found = m.Search([]byte("4"))
	value = entry.Value
	if !found || string(value) != "newfour" {
		t.Error("Expected newfour")
	}
	entry, found = m.Search([]byte("7"))
	value = entry.Value
	if !found || string(value) != "newseven" {
		t.Error("Expected newseven")
	}
	entry, found = m.Search([]byte("12"))
	value = entry.Value
	if !found || string(value) != "newtwelve" {
		t.Error("Expected newtwelve")
	}
	entry, found = m.Search([]byte("10"))
	value = entry.Value
	if !found || string(value) != "ten" {
		t.Error("Expected ten")
	}
	_, found = m.Search([]byte("2"))
	if found {
		t.Error("Expected not found for key 2")
	}
	fmt.Println("Search test passed")

	fmt.Println("Deleting 4, 10, and 12 from Memtable:")
	m.Delete([]byte("4"))
	ent, _ := m.Search([]byte("4"))
	if !ent.Tombstone {
		t.Error("Expected not found for deleted key 4")
	}
	m.Delete([]byte("10"))
	ent, _ = m.Search([]byte("10"))
	if !ent.Tombstone {
		t.Error("Expected not found for deleted key 10")
	}
	m.Delete([]byte("12"))
	ent, _ = m.Search([]byte("12"))
	if !ent.Tombstone {
		t.Error("Expected not found for deleted key 12")
	}

	fmt.Println("Memtable contents after CRUD operations:")
	m.Print()
}

func TestMemtables(t *testing.T) {
	cf := config.Config{Memtable: config.MemtableConfig{
		NumberOfMemtables: 2,
		NumberOfEntries:   5,
		Structure:         "skiplist",
	},
		Skiplist: config.SkiplistConfig{
			MaxHeight: 3},
		BTree: config.BTreeConfig{
			MinSize: 16,
		},
	}
	ms := NewMemtables(&cf)
	for i := 0; i < cf.Memtable.NumberOfMemtables; i++ {
		fmt.Printf("Memtable %d:\n", i)
		ms.Memtables[i].Print()
	}
	fmt.Println("Adding entries to Memtables:")
	ms.Update([]byte("1"), []byte("one"), 1, false)
	ms.Update([]byte("2"), []byte("two"), 2, false)
	ms.Update([]byte("3"), []byte("three"), 3, false)
	ms.Update([]byte("4"), []byte("four"), 4, false)
	ms.Update([]byte("5"), []byte("five"), 5, false)
	ms.Update([]byte("6"), []byte("six"), 6, false)
	ms.Update([]byte("7"), []byte("seven"), 7, false)
	ms.Update([]byte("8"), []byte("eight"), 8, false)
	for i := 0; i < cf.Memtable.NumberOfMemtables; i++ {
		fmt.Printf("Memtable %d after updates:\n", i)
		ms.Memtables[i].Print()
	}
	fmt.Println("Updating keys in Memtables:")
	ms.Update([]byte("6"), []byte("newsix"), 15, false)
	ms.Update([]byte("7"), []byte("newseven"), 16, false)
	ms.Update([]byte("8"), []byte("neweight"), 17, false)
	for i := 0; i < cf.Memtable.NumberOfMemtables; i++ {
		fmt.Printf("Memtable %d after updates:\n", i)
		ms.Memtables[i].Print()
	}
	fmt.Println("Searching keys in Memtables:")
	_, found := ms.Search([]byte("1"))

	if !found {
		t.Error("Expected to find key 1")
	}
	fmt.Println("Deleting keys from Memtables:")
	ms.Delete([]byte("7"))
	ent, _ := ms.Search([]byte("7"))
	if !ent.Tombstone {
		t.Error("Expected not found for deleted key 7")
	}

	fmt.Println("Memtables contents after CRUD operations:")
	for i := 0; i < cf.Memtable.NumberOfMemtables; i++ {
		fmt.Printf("Memtable %d:\n", i)
		ms.Memtables[i].Print()
	}

}

func TestMemtableIterate(t *testing.T) {
	conf := &config.Config{
		Memtable: config.MemtableConfig{
			NumberOfMemtables: 4,
			NumberOfEntries:   5,
			Structure:         "skiplist",
		},
		Skiplist: config.SkiplistConfig{
			MaxHeight: 16,
		},
		Block: config.BlockConfig{
			BlockSize: 4096,
		},
	}

	memtables := NewMemtables(conf)
	memtables.Update([]byte("key1"), []byte("value1"), 1, false)
	memtables.Update([]byte("key2"), []byte("value2"), 2, false)
	memtables.Update([]byte("key3"), []byte("value3"), 3, false)
	memtables.Update([]byte("key4"), []byte("value4"), 4, false)
	memtables.Update([]byte("key5"), []byte("value5"), 5, false)
	memtables.Update([]byte("key6"), []byte("value6"), 6, false)
	memtables.Update([]byte("key7"), []byte("value7"), 7, false)
	memtables.Update([]byte("key8"), []byte("value8"), 8, false)
	memtables.Update([]byte("key9"), []byte("value9"), 9, false)
	memtables.Update([]byte("key10"), []byte("value10"), 10, false)

	for i := 0; i < conf.Memtable.NumberOfMemtables; i++ {
		iterator := memtables.Memtables[i].NewMemtableIterator()
		if iterator == nil {
			break
		}
		// Iterate through all entries
		for {
			entry, ok := iterator.Next()
			if !ok {
				break
			}
			fmt.Printf("Key: %s, Value: %s\n", entry.Key, entry.Value)
		}

	}
	for i := 0; i < conf.Memtable.NumberOfMemtables; i++ {
		prefixIterator := memtables.Memtables[i].PrefixIterate("key1")
		if prefixIterator == nil {
			continue
		}
		fmt.Printf("Prefix iterator for Memtable %d:\n", i)
		for {
			entry, ok := prefixIterator.Next()
			if !ok {
				break
			}
			fmt.Printf("Key: %s, Value: %s\n", entry.Key, entry.Value)
		}
		prefixIterator.Stop()
	}

	rangeIterator := memtables.Memtables[0].RangeIterate([]byte("key3"), []byte("key8"))
	if rangeIterator == nil {
		return
	}
	fmt.Printf("Range iterator for Memtable %d:\n", 0)
	for {
		entry, ok := rangeIterator.Next()
		if !ok {
			break
		}
		fmt.Printf("Key: %s, Value: %s\n", entry.Key, entry.Value)
	}
	rangeIterator.Stop()

}

func TestMemtablesIterator(t *testing.T) {
	conf := &config.Config{
		Memtable: config.MemtableConfig{
			NumberOfMemtables: 4,
			NumberOfEntries:   5,
			Structure:         "skiplist",
		},
		Skiplist: config.SkiplistConfig{
			MaxHeight: 16,
		},
		Block: config.BlockConfig{
			BlockSize: 4096,
		},
	}

	memtables := NewMemtables(conf)
	memtables.Update([]byte("key1"), []byte("value1"), 1, false)
	memtables.Update([]byte("key2"), []byte("value2"), 2, false)
	memtables.Update([]byte("key3"), []byte("value3"), 3, false)
	memtables.Update([]byte("key4"), []byte("value4"), 4, false)
	memtables.Update([]byte("key5"), []byte("value5"), 5, false)
	memtables.Update([]byte("key6"), []byte("value6"), 6, false)
	memtables.Update([]byte("key7"), []byte("value7"), 7, false)
	memtables.Update([]byte("key3"), []byte("value8"), 8, false)
	memtables.Update([]byte("key2"), []byte("value9"), 9, false)
	memtables.Update([]byte("key1"), []byte("value10"), 10, false)
	println("Iterating:")
	iter := memtables.NewMemtablesIterator()

	if iter == nil {
		t.Error("Expected MemtablesIterator to be created")
		return
	}

	for {
		entry, ok := iter.Next()
		if !ok {
			break
		}
		fmt.Printf("Key: %s, Value: %s\n", entry.Key, entry.Value)

	}

	piter := memtables.PrefixIterate("key1")
	if piter == nil {
		t.Error("Expected MemtablePrefixIterator to be created")
		return
	}
	for {
		entry, ok := piter.Next()
		if !ok {
			break
		}
		fmt.Printf("Prefix Key: %s, Value: %s\n", entry.Key, entry.Value)
	}
	riter := memtables.RangeIterate([]byte("key3"), []byte("key8"))
	if riter == nil {
		t.Error("Expected MemtableRangeIterator to be created")
		return
	}
	for {
		entry, ok := riter.Next()
		if !ok {
			break
		}
		fmt.Printf("Range Key: %s, Value: %s\n", entry.Key, entry.Value)
	}
}

func TestMemtablesScan(t *testing.T) {
	conf := &config.Config{
		Memtable: config.MemtableConfig{
			NumberOfMemtables: 4,
			NumberOfEntries:   5,
			Structure:         "skiplist",
		},
		Skiplist: config.SkiplistConfig{
			MaxHeight: 16,
		},
		Block: config.BlockConfig{
			BlockSize: 4096,
		},
	}

	memtables := NewMemtables(conf)
	memtables.Update([]byte("key1"), []byte("value1"), 1, false)
	memtables.Update([]byte("key2"), []byte("value2"), 2, false)
	memtables.Update([]byte("key3"), []byte("value3"), 3, false)
	memtables.Update([]byte("key4"), []byte("value4"), 4, false)
	memtables.Update([]byte("key5"), []byte("value5"), 5, false)
	memtables.Update([]byte("key6"), []byte("value6"), 6, false)
	memtables.Update([]byte("key7"), []byte("value7"), 7, false)
	memtables.Update([]byte("key8"), []byte("value8"), 8, false)
	memtables.Update([]byte("key9"), []byte("value9"), 9, false)
	memtables.Update([]byte("key10"), []byte("value10"), 10, false)

	fmt.Println("Range Scan from key3 to key8:")
	rangeEntries := memtables.RangeScan([]byte("key3"), []byte("key8"), 1, 5)
	for _, entry := range rangeEntries {
		fmt.Printf("Key: %s, Value: %s\n", entry.Key, entry.Value)
	}

	fmt.Println("\nPrefix Scan for key1:")
	prefixEntries := memtables.PrefixScan("key1", 1, 5)
	for _, entry := range prefixEntries {
		fmt.Printf("Key: %s, Value: %s\n", entry.Key, entry.Value)
	}
}
