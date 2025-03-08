package elastic1

import (
	"hash/maphash"
	"math"
)

const prime32 = 0xfffffffb // Just the last 32-bit prime number

// TODO: go run -gcflags="-d=ssa/check_bce" example2.go
// TODO: figure out the bankOverflowFactor

// NewHashTableDefault creates a new hash table with default parameters.
func NewHashTableDefault(capacity int) *HashTable {
	return NewHashTable(capacity, 0.1, 0.75, 200)
}

// NewHashTable creates a new hash table. Capacity parameter is a size of the table, must be positive.
//
// Delta is a fraction of free slots in table, must be in range (0,1).
//
// bankShrink controls the distribution of data banks in table: the lower the ratio, the quicker data banks shrink
// towards the end of the table. Must be in range (0, 1).
//
// bankOverflowFactor controls how much data should be inserted to a bank before the next bank is used. Must be non-negative.
// It's the c parameter in paper.
func NewHashTable(capacity int, delta, bankShrink, bankOverflowFactor float64) *HashTable {
	// We use the power of 2 as bank size only for convenience. So they will have sizes, say, 16, 8, 4, 2, 1.
	// Actually, the size may have base other than 2.
	banks := math.Ceil(math.Log2(float64(capacity + 1)))
	if banks == 0 {
		banks = 1
	}
	bbanks := make([]*Bbank, int(banks))
	s := 1
	for i := int(banks) - 1; i >= 0; i-- {
		bbanks[i] = &Bbank{OriginalSize: s}
		if i < int(banks)-1 {
			bbanks[i].Next = bbanks[i+1]
		}
		s *= 2
	}
	return &HashTable{
		Hasher:             defaultHasher(maphash.MakeSeed()),
		BankOverflowFactor: bankOverflowFactor,
		BankShrink:         bankShrink,
		Capacity:           capacity,
		Delta:              delta,
		Banks:              bbanks,
	}
}

// HashTable is an implementation variant of hash table with elastic hashing algorithm.
//
// In this implementation, banks count are calculated on creation and fixed.
// Table size is not limited, since the banks has dynamic size. Every bank is the linked list of slots.
//
// All data slots are divided into banks with dynamic size geometrically decreasing by the power of 2.
// Every slot stores a key, value, and the first byte of hash to speed up the key probing.
//
// In this implementation, the geometric decreasing rate fully depends on hash function quality, specifically on
// its even distribution of values, because the bank index is calculated roughly as “log2(hash)”.
// Therefore, if the hash function is uniform, banks obey to the geometric progression “2^i”.
//
// To make the insert or lookup, first we select a bank based on the key hash value.
// On collision, we do the linear probing starting from the first bank slot.
// For inserts, based on selected bank metrics we decide whether we should insert the item to this bank
// or the next one. If we choose the next bank, we do the same for it, and so on, until the item is inserted.
type HashTable struct {
	Hasher func(b []byte) uint32

	BankOverflowFactor float64 // bucket fullness coefficient for the next bucket usage, c parameter in paper
	BankShrink         float64 // rate of bucket size decrease, 3/4 in paper as default
	Capacity           int     // total number of slots, n parameter in paper
	Inserts            int     // Metrics of total number of occupied slots
	Delta              float64 // δ parameter in paper
	Banks              []*Bbank
}

// Insert inserts a new key-value pair into the hash table. It must be called once for every key, because it makes
// inserts even if the key already exists. Otherwise, the hash table could contain several items with identical
// keys.
//
// To set a value for a key, as any “map” type does, use Set method.
func (t *HashTable) Insert(key []byte, value any) {
	if t.Inserts >= t.Capacity {
		panic("capacity exceeded")
	}
	insert(t, key, value)
}

// Set sets a value for a key. If the key already exists, it updates the value. Otherwise, it inserts a new key-value
// pair.
func (t *HashTable) Set(key []byte, value any) bool {
	slot, ok := lookup(t, key)
	if ok {
		slot.Value = value
	} else {
		t.Insert(key, value)
	}
	return ok
}

// Get returns a value for a key. If the key does not exist, it returns nil and false.
func (t *HashTable) Get(key []byte) (any, bool) {
	if slot, ok := lookup(t, key); ok {
		return slot.Value, true
	}
	return nil, false
}

// Len returns the number of elements in the hash table.
func (t *HashTable) Len() int {
	return t.Inserts
}

// Cap returns the capacity of the hash table.
func (t *HashTable) Cap() int {
	return t.Capacity
}

func defaultHasher(seed maphash.Seed) func(b []byte) uint32 {
	return func(b []byte) uint32 {
		h := maphash.Bytes(seed, b)
		// fold 64-bit hash to 32-bit
		return uint32(h % prime32)
	}
}
