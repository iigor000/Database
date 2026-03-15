# NoSQL Database Implementation

A high-performance NoSQL key-value database written in Go, featuring advanced data structures and algorithms for efficient storage, retrieval, and analysis of data. This project demonstrates the practical application of algorithms and data structures in modern database systems.

## Table of Contents
- [Architecture Overview](#architecture-overview)
- [Core Components](#core-components)
- [Data Structures Used](#data-structures-used)
- [Key Algorithms](#key-algorithms)
- [Advanced Features](#advanced-features)
- [Usage](#usage)
- [Configuration](#configuration)

## Architecture Overview

The database follows a **Log-Structured Merge (LSM) Tree** architecture combined with an in-memory caching layer for optimal performance. Here's how the complete data flow works:

```
User Operations (Put/Get/Delete)
    ↓
Rate Limiting (Token Bucket)
    ↓
Write-Ahead Log (WAL) - Crash Recovery
    ↓
In-Memory Memtables (Skip List/BTree)
    ↓
Cache Layer - Frequently Accessed Data
    ↓
LSM Tree with SSTables (on-disk sorted storage)
    ↓
Compaction (Leveled or Size-Tiered)
```

## Core Components

### 1. **Write-Ahead Log (WAL)**
- **Purpose**: Ensures durability and crash recovery
- **How it works**: Every write operation is first logged to disk before being applied to in-memory structures
- **Location**: [`structures/writeAheadLog/writeAheadLog.go`](structures/writeAheadLog/writeAheadLog.go)
- **Features**:
  - Segmented logging for efficient space management
  - Supports both regular records and tombstones (for deletions)
  - Records are replayed on database startup to restore state

### 2. **Memtables - In-Memory Data Structure**
- **Purpose**: Fast in-memory storage of recent writes
- **Data Structures**: 
  - **Skip List** (default) - O(log n) operations with better cache locality
  - **B-Tree** (optional) - Balanced tree structure
  - **HashMap** (optional) - Hash-based lookup
- **How it works**:
  - Multiple memtables are maintained in sequence
  - When a memtable reaches capacity, it's flushed to disk as an SSTable
  - Old memtables are removed from WAL
- **Location**: [`structures/memtable/memtable.go`](structures/memtable/memtable.go)

### 3. **Cache Layer**
- **Purpose**: Accelerates read operations by caching frequently accessed entries
- **Algorithm**: Configurable cache replacement policy
- **How it works**:
  - When data is retrieved from LSM tree, it's automatically cached
  - Cache is checked before querying LSM tree on reads
  - Reduces disk I/O significantly for hot data
- **Location**: [`structures/cache/cache.go`](structures/cache/cache.go)

### 4. **LSM Tree - Persistent Storage**
- **Purpose**: Efficient long-term storage with optimal read/write tradeoff
- **Structure**: Multiple levels of SSTables
- **How it works**:
  - Level 1: Contains flushed memtables
  - Higher levels: Compacted from lower levels
  - Compaction process keeps SSTables sorted and reduces redundancy
- **Location**: [`structures/lsmtree/lsmtree.go`](structures/lsmtree/lsmtree.go)

### 5. **SSTables (Sorted String Tables)**
- **Purpose**: Sorted, immutable storage on disk
- **Components**:
  - **Data Block**: Actual key-value pairs
  - **Index Block**: Fast key lookup within SSTable
  - **Summary Block**: High-level range information for binary search
  - **Bloom Filter**: Fast negative lookups (skip searching if key doesn't exist)
  - **Merkle Tree**: Data integrity verification
- **Features**:
  - Block-based organization for efficient I/O
  - Compression support for reduced storage
  - Configurable block size
- **Location**: [`structures/sstable/sstable.go`](structures/sstable/sstable.go)

### 6. **Block Organization & Management**
- **Purpose**: Efficient disk I/O with block-level caching
- **Components**:
  - **Block Manager**: Handles disk positioning and I/O
  - **Block Cache**: Caches frequently accessed disk blocks
  - **Cached Block Manager**: Combines both for optimal performance
- **How it works**:
  - Data is organized into fixed-size blocks
  - Block cache reduces repeated disk reads
  - Locality principle improves cache hit rates
- **Location**: [`structures/block_organization/block_manager.go`](structures/block_organization/block_manager.go)

### 7. **Compression Dictionary**
- **Purpose**: Reduces storage size through compression
- **How it works**:
  - Tracks frequent keys and values
  - Creates a dictionary for compression
  - Compresses SSTable data using the dictionary
- **Location**: [`structures/compression/compression.go`](structures/compression/compression.go)

## Data Structures Used

### Core Data Structures

| Structure | Purpose | Location | Time Complexity |
|-----------|---------|----------|-----------------|
| **Skip List** | Memtable index for fast sorted access | [`structures/skiplist/skiplist.go`](structures/skiplist/skiplist.go) | O(log n) avg |
| **B-Tree** | Alternative memtable structure | [`structures/btree/btree.go`](structures/btree/btree.go) | O(log n) |
| **HashMap** | Fast key-value lookup | [`structures/hashmap/hashmap.go`](structures/hashmap/hashmap.go) | O(1) avg |

### Probabilistic Data Structures

| Structure | Purpose | Use Case | Space |
|-----------|---------|----------|-------|
| **Bloom Filter** | Membership testing | Check if key exists before disk lookup | O(m bits) |
| **Count-Min Sketch** | Frequency estimation | Track element frequencies with error bounds | O(k × w) |
| **HyperLogLog** | Cardinality estimation | Estimate distinct element count | O(log log n) |

### Security & Integrity

| Structure | Purpose | Use Case |
|-----------|---------|----------|
| **SimHash** | Text fingerprinting | Detect similar documents/texts |
| **Merkle Tree** | Data integrity | Verify SSTable integrity, detect corruption |

## Key Algorithms

### Database Operations

#### **Put Operation**
```
1. Rate limit check (Token Bucket)
2. Validate key (not reserved)
3. Append to WAL (durability)
4. Update compression dictionary
5. Insert into active memtable
6. If memtable full:
   - Flush to SSTable
   - Trigger LSM compaction if needed
   - Update cache for flushed entries
```

#### **Get Operation**
```
1. Rate limit check
2. Search in memtables (O(log n))
3. If not found, check cache (O(1))
4. If not found, search LSM tree:
   - Check Bloom Filter first (quick negative check)
   - Binary search through SSTable index
   - Retrieve from data block
5. Return with highest timestamp if multiple versions
6. Cache the result for future access
```

#### **Delete Operation**
```
1. Write tombstone to WAL
2. Mark entry for deletion in memtable
3. On compaction, tombstones purge old data
```

### LSM Tree Compaction

The database supports **two compaction strategies**:

#### **Size-Tiered Compaction**
- **When**: When multiple SSTables on same level reach size threshold
- **How**: Merge all overlapping SSTables into single next-level table
- **Pros**: Write-optimized, minimal writes during compaction
- **Cons**: Higher read amplification

#### **Leveled Compaction**
- **When**: Continuously keeps each level at size ratio limit
- **How**: Merges one level with overlapping SSTables from next level
- **Pros**: Lower read amplification, more predictable
- **Cons**: Higher write overhead
- **Location**: [`structures/lsmtree/lsmtree.go`](structures/lsmtree/lsmtree.go#L87)

## Advanced Features

### 1. **Bloom Filters**
Create probabilistic filters for fast membership testing:
```go
NewBloomFilter(name, universe_size, error_probability)
AddToBloomFilter(name, element)
CheckBloomFilter(name, element)
```
- **Application**: Before searching SSTable, check if key might exist
- **Benefit**: 95% of lookups return instant "not found" response

### 2. **Count-Min Sketch**
Estimate element frequencies with bounded error:
```go
CreateCMS(name, epsilon, delta)
AddToCMS(name, element)
EstimateCMS(name, element)
```
- **Application**: Track access patterns, find hot keys
- **Benefit**: O(1) updates and queries with minimal memory usage

### 3. **HyperLogLog**
Approximate cardinality estimation:
```go
CreateHLL(name, precision)
AddToHLL(name, element)
EstimateHLL(name)
```
- **Application**: Count unique users/IPs efficiently
- **Benefit**: Constant memory regardless of dataset size

### 4. **Digital Fingerprinting (SimHash)**
Generate fingerprints for similarity detection:
```go
AddSHFingerprint(name, text)
GetHemmingDistance(fp1, fp2)
```
- **Application**: Find similar documents or detect near-duplicates
- **Benefit**: Hamming distance directly correlates with text similarity

### 5. **Merkle Tree Validation**
Verify data integrity:
```go
ValidateMerkleTree(generation, level)
```
- **Application**: Detect corruption at block level
- **Benefit**: Hierarchical verification, efficient batch validation

### 6. **Pagination Support**
Efficient range and prefix queries:
```go
PrefixScan(prefix, pageNumber, pageSize, inMemory)
RangeScan(start, end, pageNumber, pageSize, inMemory)
```
- **Features**:
  - Page-based results to limit memory usage
  - Skip list iterators for efficient sequential access
  - Binary search in sorted data

### 7. **Rate Limiting (Token Bucket)**
Per-user rate limiting to prevent abuse:
```go
CreateBucket(username)
CheckBucket(username)
```
- **Algorithm**: Token bucket with configurable refill rate
- **Application**: Prevent single user from monopolizing resources
- **Root user**: Bypasses all rate limiting

## Usage

### Starting the Database
```bash
go run main.go
username: myuser
```

### Basic Operations
```
put key1 value1        # Store key-value pair
get key1               # Retrieve value
delete key1            # Delete entry
```

### Advanced Features
```
addbl filter1 100 0.01         # Create Bloom filter with 1% error
addtocms sketch1 item1         # Add to Count-Min Sketch
addhll hll1 5                  # Create HyperLogLog precision 5
addfp fp1 "text content"       # Create SimHash fingerprint
prefix_scan prefix 1 10 false  # Scan matching prefix, page 1, size 10
range_scan a z 1 10 false      # Scan range a-z
validate 1 1                   # Validate SSTable generation 1, level 1
```

## Configuration

Edit `config/config.json` to tune:

```json
{
  "block": {
    "block_size": 4096                    // Disk I/O unit size
  },
  "memtable": {
    "num": 2,                             // Number of memtables
    "num_entries": 10000,                 // Max entries per memtable
    "struct": "skiplist"                  // "skiplist", "btree", or "hashmap"
  },
  "sstable": {
    "use_compression": true,              // Enable dictionary compression
    "summary_level": 8,                   // Summary block size
    "directory": "sstables"               // Storage directory
  },
  "cache": {
    "capacity": 1000,                     // Max entries in cache
    "policy": "lru"                       // Eviction policy
  },
  "lsmtree": {
    "max_level": 5,                       // Maximum LSM levels
    "compaction_algorithm": "leveled"     // "leveled" or "size_tiered"
  },
  "token_bucket": {
    "refill_rate": 100,                   // Tokens per second
    "start_tokens": 10000,                // Initial tokens
    "max_tokens": 50000                   // Bucket capacity
  }
}
```

## Key Features Summary

✅ **ACID Properties**:
- Atomicity: WAL ensures all-or-nothing writes
- Consistency: Merkle tree validation detects corruption
- Isolation: Timestamp-based versioning
- Durability: Write-ahead logging

✅ **Performance Optimizations**:
- LSM Tree for write-efficient storage
- Multi-level caching (cache + block cache)
- Bloom filters for negative lookups
- Compression for reduced storage

✅ **Advanced Analytics**:
- Cardinality estimation (HyperLogLog)
- Frequency tracking (Count-Min Sketch)
- Similarity detection (SimHash)
- Membership testing (Bloom Filter)

✅ **Operational Features**:
- Per-user rate limiting
- Pagination for large result sets
- Data integrity verification
- Crash recovery via WAL

## Algorithm Complexity

### Core Operations
| Operation | Average | Worst Case |
|-----------|---------|-----------|
| Put | O(log n) | O(log n) |
| Get | O(log n) | O(log n) |
| Delete | O(log n) | O(log n) |
| Prefix Scan | O(k log n) | O(n) |
| Range Scan | O(k log n) | O(n) |

Where n = total entries, k = result size

### Space Complexity
| Component | Space |
|-----------|-------|
| Memtable | O(n) |
| Cache | O(c) where c = capacity |
| Bloom Filter | O(m bits) |
| HyperLogLog | O(log log n) |
| Count-Min Sketch | O(1/ε × log(1/δ)) |

---

**This is an educational implementation demonstrating advanced data structures and algorithms used in production NoSQL databases.**
