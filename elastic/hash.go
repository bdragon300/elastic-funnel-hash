package elastic

import (
	"fmt"
	"hash/maphash"
	"math"
)

const prime32 = 0xfffffffb // Just the last 32-bit prime number

// TODO: go run -gcflags="-d=ssa/check_bce" example2.go

// NewHashTableDefault creates a new hash table with default parameters.
func NewHashTableDefault(capacity int) *HashTable {
	return NewHashTable(capacity, 0.1, 3/4, 200)
}

// NewHashTable creates a new hash table. Capacity parameter is a total elements.
//
// Fullness is a fraction of free slots in table, must be in range (0,1). It's the δ parameter in paper.
//
// ShrinkRatio controls the distribution of data banks in table: the lower the ratio, the quicker data banks shrink
// towards the end of the table. Must be in range (0, 1).
//
// FillRate controls how full a data bank should be filled up before the next bank is used. Must be non-negative.
// It's the c parameter in paper.
func NewHashTable(capacity int, fullness, shrinkRatio, fillRate float64) *HashTable {
	if capacity <= 0 {
		panic(fmt.Errorf("capacity must be positive"))
	}
	if fullness <= 0 || fullness >= 1 {
		panic(fmt.Errorf("fullness must be in range (0, 1)"))
	}
	if shrinkRatio <= 0 || shrinkRatio >= 1 {
		panic("shrinkRatio must be in range (0, 1)")
	}
	if fillRate < 0 {
		panic("fillRate must be non-negative")
	}

	// We use the power of 2 as bank size only for convenience.
	// So they will have sizes, say, 16, 8, 4, 2, 1.
	// Actually, the size may have any base.
	banks := math.Ceil(math.Log2(float64(capacity + 1)))
	capacity = int(math.Pow(2, banks))
	return &HashTable{
		Hasher:      defaultHasher(maphash.MakeSeed()),
		fillRate:    fillRate,
		shrinkRatio: shrinkRatio,
		capacity:    capacity,
		inserts:     make([]int, int(banks)),
		fullness:    fullness,
		slots:       make([]*bslot, capacity),
	}
}

// HashTable is an implementation variant of hash table with elastic hashing algorithm.
//
// Data slots are divided into data banks, with bank size decreasing by the power of 2.
// Slots are kept in one array, data banks are just structures that virtually divide it to control the probing
// sequence over it. Slot array is fully allocated upon creation.
//
// Inserts and lookups start from the hashed position, and then follow the probing sequence towards the end of the table.
// If insert operation in this case failed to find a free slot, it starts to process the other part from the
// beginning of the table.
type HashTable struct {
	Hasher func(b []byte) uint32

	fillRate     float64 // data bank cutoff value for the next bank usage, c parameter in paper
	shrinkRatio  float64 // rate of bank size decrease, 3/4 in paper
	capacity     int     // total number of slots, n parameter in paper
	inserts      []int   // total number of occupied slots metrics in each bank
	totalInserts int     // total number of occupied slots metric
	fullness     float64 // δ parameter in paper
	slots        []*bslot
}

// Insert inserts a new key-value pair into the hash table. It must be called once for every key, because it makes
// inserts even if the key already exists. Otherwise, the hash table could contain several items with identical
// keys.
//
// To set a value for a key, as any “map” type does, use Set method.
func (t *HashTable) Insert(key []byte, value any) {
	if t.totalInserts >= t.capacity {
		panic("capacity exceeded")
	}
	insert(t, key, value)
}

// Set sets a value for a key. If the key already exists, it updates the value. Otherwise, it inserts a new key-value
// pair.
func (t *HashTable) Set(key []byte, value any) bool {
	slot, ok := lookup(t, key)
	if ok {
		slot.value = value
	} else {
		t.Insert(key, value)
	}
	return ok
}

// Get returns a value for a key. If the key does not exist, it returns nil and false.
func (t *HashTable) Get(key []byte) (any, bool) {
	if slot, ok := lookup(t, key); ok {
		return slot.value, true
	}
	return nil, false
}

// Len returns the number of elements in the hash table.
func (t *HashTable) Len() int {
	return t.totalInserts
}

// Cap returns the capacity of the hash table.
func (t *HashTable) Cap() int {
	return t.capacity
}

func defaultHasher(seed maphash.Seed) func(b []byte) uint32 {
	return func(b []byte) uint32 {
		h := maphash.Bytes(seed, b)
		// fold 64-bit hash to 32-bit
		return uint32(h % prime32)
	}
}
