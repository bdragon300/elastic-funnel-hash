package funnel

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

const (
	prime32        = 0xfffffffb // Just the last 32-bit prime number
	banksMinCount  = 10         // Minimum banks count excluding overflow
	minShrinkRatio = 1 / 2
)

// NewHashTableDefault creates a new hash table with default parameters.
func NewHashTableDefault(capacity int) *HashTable {
	return NewHashTable(capacity, 0.1, 3/4)
}

// NewHashTable creates a new hash table. Capacity parameter is a total elements. Delta (δ) is a fill factor, i.e.
// fraction of free slots, must be in range (0,1).
// ShrinkRatio controls the distribution of buckets in data banks: the lower the ratio, the quicker data banks shrink
// towards the end of the table. Must be in range [1/2, 1).
func NewHashTable(capacity int, delta, shrinkRatio float64) *HashTable {
	if shrinkRatio < minShrinkRatio || shrinkRatio >= 1 {
		panic(fmt.Errorf("shrinkRatio must be in range [%v, 1)", minShrinkRatio))
	}
	if delta <= 0 || delta >= 1 {
		panic(fmt.Errorf("delta must be in range (0, 1)"))
	}
	if capacity <= 0 {
		panic(fmt.Errorf("capacity must be positive"))
	}

	alpha := math.Ceil(4*math.Log2(1/delta)) + banksMinCount // Banks count
	beta := math.Ceil(2 * math.Log2(1/delta))                // Bucket size
	// Average on range: ⌊δn*shrinkRatio⌋ ≥ |Aα+1| ≥ ⌈δn*minShrinkRatio⌉
	overflowCapacity := (math.Floor(delta*float64(capacity)*shrinkRatio) + math.Ceil(delta*float64(capacity)*minShrinkRatio)) / 2

	sizesSeqSum := (1 - math.Pow(shrinkRatio, alpha-1)) / (1 - shrinkRatio) // Sum of geometric sequence: Σ(shrinkRatio)^x, i=0..α-1
	slots := int(math.Ceil((float64(capacity) - overflowCapacity) / sizesSeqSum))
	slots = slots + int(beta) - slots%int(beta) // Round up to the nearest multiple of β

	ovfLogLogn := math.Log2(math.Log2(float64(capacity)))

	ovf2BucketSize := 2 * ovfLogLogn // Overflow banks have their own bucket size
	ovf2Slots := int(math.Round(float64(overflowCapacity) / 2))
	ovf2Slots = ovf2Slots + int(ovf2BucketSize) - ovf2Slots%int(ovf2BucketSize) // Round up to the nearest multiple of bucket size

	ovf1Slots := int(overflowCapacity) - ovf2Slots
	ovf1Rnd := rand.NewChaCha8([32]byte{})
	ovf1Seed := uint32(time.Now().UnixNano() % prime32)

	return &HashTable{
		alpha:       alpha,
		bucketSize:  uint32(beta),
		shrinkRatio: shrinkRatio,
		capacity:    capacity,
		root: &bbank{
			slots: make([]*bslot, slots),
		},
		overflow1: &boverflow{
			slots:   make([]*bslot, ovf1Slots),
			rnd:     ovf1Rnd,
			seed:    ovf1Seed,
			loglogn: ovfLogLogn,
		},
		overflow2: &boverflow{
			slots:   make([]*bslot, ovf2Slots),
			loglogn: ovfLogLogn,
		},
	}
}

type HashTable struct {
	Hasher      func(b []byte) uint32
	alpha       float64
	bucketSize  uint32
	shrinkRatio float64
	capacity    int
	inserts     int

	root *bbank
	// overflow1 is an overflow bucket (the first half of Aα+1 bank). Hash table with random probes
	overflow1 *boverflow
	// overflow2 is an overflow bucket (the second half of Aα+1 bank). Two-choice hashing
	overflow2 *boverflow
}

// Insert inserts a new key-value pair into the hash table. It must be called once for every key, because it makes
// inserts even if the key already exists. In this case, the hash table will contain two or more items with identical
// keys.
//
// To set a value for a key, as any “map” type does, use Set method.
func (t *HashTable) Insert(key []byte, value any) {
	if t.inserts >= t.capacity {
		panic("hash table is full")
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

// Cap returns the capacity of the hash table.
func (t *HashTable) Cap() int {
	return t.capacity
}

// Len returns the number of elements in the hash table.
func (t *HashTable) Len() int {
	return t.inserts
}
