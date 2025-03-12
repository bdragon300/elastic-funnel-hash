package elastic

import (
	"fmt"
	"hash/maphash"
	"math"
	"math/rand/v2"
)

const prime32 = 0xfffffffb // Just the last 32-bit prime number

// TODO: go run -gcflags="-d=ssa/check_bce" example2.go

// NewHashTableDefault creates a new hash table with default parameters.
func NewHashTableDefault(capacity int) *HashTable {
	return NewHashTable(capacity, 0.1, 0.75, 200)
}

// NewHashTable creates a new hash table. Capacity parameter is a total elements.
//
// Delta is a fraction of slots to keep free in table. Affects to performance. Must be in range (0,1).
//
// bank2Occupation is fraction of bank slots reserved for insertions while being 2nd bank in a pair (as Ai+1 bank).
// Also applies in the table's first bank. Must be in range (0,1). Default in the Paper is 0.75.
//
// bank1FillFactor controls how quickly the 1st bank in a pair (Ai bank) is filled with inserted items.
// Must be non-negative. It's the c parameter in Paper.
func NewHashTable(capacity int, delta, bank2Occupation, bank1FillFactor float64) *HashTable {
	if capacity <= 0 {
		panic(fmt.Errorf("capacity must be positive"))
	}
	if delta <= 0 || delta >= 1 {
		panic(fmt.Errorf("delta must be in range (0, 1)"))
	}
	if bank2Occupation <= 0 || bank2Occupation >= 1 {
		panic(fmt.Errorf("bank2Occupation must be in range (0, 1)"))
	}
	if bank1FillFactor <= 0 {
		panic(fmt.Errorf("bank1FillFactor must be positive"))
	}

	// We use the power of 2 as bank size only for convenience. So they will have sizes, say, 16, 8, 4, 2, 1.
	// Actually, the size may have any base.
	var banks []*Bank
	for i := 1; i < capacity; i *= 2 {
		banks = append(banks, &Bank{
			Data: make([]*Slot, i),
		})
	}
	banks = append(banks, &Bank{
		Data: make([]*Slot, int(math.Pow(2, float64(len(banks))))),
	})
	return &HashTable{
		Hasher:          defaultHasher(maphash.MakeSeed()),
		Bank1FillFactor: bank1FillFactor,
		Bank2Occupation: bank2Occupation,
		Capacity:        capacity,
		Delta:           delta,
		Banks:           banks,
	}
}

// HashTable is an implementation of hash table with elastic hashing algorithm. Table size is fixed and set on creation.
//
// All data slots are divided into banks (sub-arrays A1...Alog2(n)) with fixed size geometrically decreasing by the power of 2.
//
// Before the insertion and lookup we select a consecutive banks pair (Ai and Ai+1) to work on based on key hash.
// The exception is the first table's bank, which is used without a pair.
//
// For insertions, we decide which of banks in pair should be used based on metrics of each one. After that,
// we insert the key-value into the selected bank. Once a pair of banks become full, the subsequent insertions will fail.
//
// For lookups, we do a limited probes in the 1st bank in pair, then lookup in 2nd bank and then go back and probe the
// remaining slots in the 1st bank.
//
// To resolve collisions, we use the uniform random probing.
//
// For more information see the [Paper].
//
// [Paper]: https://arxiv.org/abs/2501.02305
type HashTable struct {
	Hasher func(b []byte) uint32

	Bank1FillFactor float64 // data bank fullness coefficient for the next bank usage, c parameter in Paper
	Bank2Occupation float64 // rate of bank size decrease, 3/4 in Paper
	Capacity        int     // total number of slots, n parameter in Paper
	Inserts         int     // Metric of total number of occupied slots
	Delta           float64 // δ parameter in Paper
	Banks           []*Bank
	Rnd, Rnd2       *rand.ChaCha8
}

// Insert inserts a new key-value pair into the hash table. It does not deduplicate keys, so if the key already exists,
// it will be inserted again.
//
// To set a value for a key, as any “map” type does, use Set method.
func (t *HashTable) Insert(key []byte, value any) {
	if t.Inserts >= t.Capacity {
		panic("capacity exceeded")
	}
	hsh := t.Hasher(key)
	slot := insert(t, hsh, key, value)
	if slot == nil {
		panic("no free space")
	}
}

// Set sets a value for a key. If the key already exists, it updates the value. Otherwise, it inserts a new key-value
// pair.
func (t *HashTable) Set(key []byte, value any) bool {
	hsh := t.Hasher(key)
	slot, ok := lookup(t, hsh, key)
	if ok {
		slot.Value = value
	} else {
		t.Insert(key, value)
	}
	return ok
}

// Get returns a value for a key. If the key does not exist, it returns nil and false.
func (t *HashTable) Get(key []byte) (any, bool) {
	hsh := t.Hasher(key)
	if slot, ok := lookup(t, hsh, key); ok {
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
