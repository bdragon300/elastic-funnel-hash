package funnel

import (
	"fmt"
	"hash/maphash"
	"math"
	"math/rand/v2"
	"time"
)

const (
	prime32             = 0xfffffffb // Just the last 32-bit prime number
	banksMinCount       = 10         // Minimum banks count excluding overflow
	minBankShrink       = 0.5
	minOverflow2Buckets = 2 // Two-choice hashing uses at least 2 buckets
)

// NewHashTableDefault creates a new hash table with default parameters.
func NewHashTableDefault(capacity int) *HashTable {
	return NewHashTable(capacity, 0.1, 0.75)
}

// NewHashTable creates a new hash table. Capacity parameter is a total elements.
//
// Delta is a fraction of free slots in table, must be in range (0,1).
//
// bankShrink controls the distribution of buckets in data banks: the lower the ratio, the quicker data banks shrink
// towards the end of the table. Must be in range [1/2, 1). The constant 3/4 in the paper.
func NewHashTable(capacity int, delta, bankShrink float64) *HashTable {
	if bankShrink < minBankShrink || bankShrink >= 1 {
		panic(fmt.Errorf("bankShrink must be in range [%v, 1)", minBankShrink))
	}
	if delta <= 0 || delta >= 1 {
		panic(fmt.Errorf("delta must be in range (0, 1)"))
	}
	if capacity <= 0 {
		panic(fmt.Errorf("capacity must be positive"))
	}

	alpha := math.Ceil(4*math.Log2(1/delta)) + banksMinCount // Banks count
	beta := math.Ceil(2 * math.Log2(1/delta))                // Bucket size
	// Average on range: ⌊δn*bankShrink⌋ ≥ |Aα+1| ≥ ⌈δn*minShrinkRatio⌉
	overflowSlots := int(math.Floor(delta*float64(capacity)*bankShrink)+math.Ceil(delta*float64(capacity)*minBankShrink)) / 2
	capacity += int(float64(capacity) * delta)
	slots := capacity - overflowSlots

	// Create the banks with non-zero size, their count could be less than α
	var bb, bb2 *Bank
	for i := 0; i < int(alpha) && slots > int(beta); i++ {
		size := float64(slots) * (1 - bankShrink)
		size = beta * math.Ceil(size/beta) // Round up to the nearest multiple of β
		b := &Bank{Size: int(size)}
		if bb2 != nil {
			bb2.Next = b
		} else {
			bb = b
		}
		bb2 = b
		slots -= int(size)
	}
	if slots < int(beta) {
		overflowSlots += slots // Give the remaining slots (if any) to the overflow bank
	}

	logLogn := math.Log2(math.Log2(float64(max(capacity, 2))))

	ovf2BucketSize := int(2 * logLogn) // Overflow banks have their own bucket size
	ovf2Slots := overflowSlots / 2
	// Disable overflow2 if it uses too few buckets, and yield the remaining space to overflow1.
	if ovf2BucketSize == 0 || ovf2Slots/ovf2BucketSize < minOverflow2Buckets {
		ovf2Slots = 0
	} else {
		ovf2Slots = int(float64(ovf2BucketSize) * math.Ceil(float64(ovf2Slots)/float64(ovf2BucketSize))) // Round up to the nearest bucket size
	}

	ovf1Slots := overflowSlots - ovf2Slots
	ovf1Rnd := rand.NewChaCha8([32]byte{})
	ovf1Seed := uint32(time.Now().UnixNano() % prime32)

	return &HashTable{
		Hasher:     defaultHasher(maphash.MakeSeed()),
		BucketSize: int(beta),
		Capacity:   capacity,
		Banks:      bb,
		Overflow1: &Overflow{
			Slots:   make([]*Slot, ovf1Slots),
			Rnd:     ovf1Rnd,
			Seed:    ovf1Seed,
			Loglogn: logLogn,
		},
		Overflow2: &Overflow{
			Slots:   make([]*Slot, ovf2Slots),
			Loglogn: logLogn,
		},
	}
}

// HashTable is an implementation of hash table with funnel hashing algorithm.
//
// Basically, all data slots are divided into three unequal parts:
//
//  1. 90-95% of data is stored in fixed count of banks, each consists of fixed size buckets. The number of buckets
//     in every bank geometrically decreases by the “shrink ratio” from table start towards the end. Bucket size, bucket
//     count and banks count are calculated upon table creation.
//  2. Overflow bucket, that actually is another separate mini-hashtable supporting the uniform random probing.
//     May occupy up to 5% of the table.
//  3. Overflow2 bucket, that is a separate mini-hashtable supporting the two-choice hashing containing the fixed size buckets.
//     May occupy up to 5% of the table.
//
// Inserts and lookups always start from the 1st bank. We probe only one bucket in every bank selected based on the key hash.
// If probing fails (bucket is full on insert or doesn't contain a key we're looking for on lookup), we hop to the next bank.
// Once the last bank is reached, we start to process the overflow1 bucket. If it fails, we continue with the
// overflow2 bucket. If the overflow2 bucket fails, the process is failed.
//
// Overflow2 bucket may be disabled if table capacity is too small.
type HashTable struct {
	Hasher func(b []byte) uint32

	BucketSize int // Bank size, β parameter in paper
	Capacity   int // total number of slots, n parameter in paper
	Inserts    int // Metric of total number of occupied slots

	Banks *Bank
	// overflow1 is an overflow bucket (the first half of Aα+1 "special array", the B subarray in paper). Hash table with random probes.
	Overflow1 *Overflow
	// overflow2 is an overflow bucket (the second half of Aα+1 "special array", the C subarray in paper). Two-choice hashing.
	Overflow2 *Overflow
}

// Insert inserts a new key-value pair into the hash table. It must be called once for every key, because it makes
// inserts even if the key already exists. Otherwise, the hash table could contain several items with identical
// keys.
//
// To set a value for a key, as any “map” type does, use Set method.
func (t *HashTable) Insert(key []byte, value any) {
	if t.Inserts >= t.Capacity {
		panic("hash table is full")
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

// Cap returns the capacity of the hash table.
func (t *HashTable) Cap() int {
	return t.Capacity
}

// Len returns the number of elements in the hash table.
func (t *HashTable) Len() int {
	return t.Inserts
}

func defaultHasher(seed maphash.Seed) func(b []byte) uint32 {
	return func(b []byte) uint32 {
		h := maphash.Bytes(seed, b)
		// fold 64-bit hash to 32-bit
		return uint32(h % prime32)
	}
}
