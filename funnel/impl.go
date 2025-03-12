package funnel

import (
	"encoding/binary"
	"math/rand/v2"
	"slices"
)

type Bank struct {
	Data []*Slot // Contains ``buckets * β'' slots
	Size int
	Next *Bank // Ai+1 bank
}

type Slot struct {
	Key     []byte
	Value   any
}

type Overflow struct {
	Slots   []*Slot
	Loglogn float64 // log2(log2(capacity))
	Seed    uint32
	Rnd     *rand.ChaCha8
}

func insert(table *HashTable, key []byte, value any) {
	hsh := table.Hasher(key)
	ok := bankInsert(table.Banks, hsh, key, value, table.BucketSize)
	if len(table.Overflow1.Slots) > 0 && !ok {
		ok = overflowUniformInsert(table.Overflow1, hsh, key, value, len(table.Overflow2.Slots) == 0)
	}
	if len(table.Overflow2.Slots) > 0 && !ok {
		hsh = table.Hasher(key) ^ table.Overflow1.Seed
		hsh2 := table.Hasher(key) ^ table.Overflow2.Seed
		ok = overflowTwoChoiceInsert(table.Overflow2, hsh, hsh2, key, value)
	}
	if !ok {
		panic("no free slots")
	}
	table.Inserts++
}

func lookup(table *HashTable, key []byte) (*Slot, bool) {
	hsh := table.Hasher(key)
	if value, ok := bankLookup(table.Banks, hsh, key, table.BucketSize); ok {
		return value, true
	}
	if len(table.Overflow1.Slots) > 0 {
		if value, ok := overflowUniformLookup(table.Overflow1, hsh, key, len(table.Overflow2.Slots) == 0); ok {
			return value, true
		}
	}
	if len(table.Overflow2.Slots) > 0 {
		hsh = table.Hasher(key) ^ table.Overflow1.Seed
		hsh2 := table.Hasher(key) ^ table.Overflow2.Seed
		return overflowTwoChoiceLookup(table.Overflow2, hsh, hsh2, key)
	}

	return nil, false
}

// bankInsert makes "attempted insertion" a key-value pair into a banks except overflow banks.
func bankInsert(bank *Bank, hsh uint32, key []byte, value any, bucketSize int) bool {
	if bank == nil {
		return false
	}
	slots := bank.Size
	if bank.Data == nil {
		bank.Data = make([]*Slot, slots)
	}

	buckets := slots / bucketSize
	bucketOffset := int(hsh%uint32(buckets)) * bucketSize
	innerOffset := int(hsh % uint32(bucketSize))

	// Linear circular probing one bucket, starting from slot depending on hash
	for j := 0; j < bucketSize; j++ {
		idx := bucketOffset + (innerOffset+j)%bucketSize
		if bank.Data[idx] == nil {
			bank.Data[idx] = newSlot(key, value)
			return true
		}
	}

	return bankInsert(bank.Next, hsh, key, value, bucketSize)
}

// bankLookup searches for a key-value pair in a banks except overflow banks.
func bankLookup(bank *Bank, hsh uint32, key []byte, bucketSize int) (*Slot, bool) {
	if bank == nil {
		return nil, false
	}
	slots := len(bank.Data)

	buckets := slots / bucketSize
	bucketOffset := int(hsh%uint32(buckets)) * bucketSize
	innerOffset := int(hsh % uint32(bucketSize))

	// Linear circular probing one bucket, starting from slot depending on hash
	for j := 0; j < bucketSize; j++ {
		idx := bucketOffset + (innerOffset+j)%bucketSize
		if bank.Data[idx] == nil {
			continue
		}
		if slices.Equal(bank.Data[idx].Key, key) {
			return bank.Data[idx], true
		}
	}

	return bankLookup(bank.Next, hsh, key, bucketSize)
}

// overflowUniformInsert tries to insert a key-value pair into the overflow1 bank. This bank behaves as a separate
// open-addressed hash table with uniform random probing. Returns true if the insertion was successful, otherwise false.
// The fullProbe is true if the insertion must probe the whole table instead of the log(log(n)) slots.
func overflowUniformInsert(ovf *Overflow, hsh uint32, key []byte, value any, fullProbe bool) bool {
	var seed [32]byte
	binary.BigEndian.PutUint32(seed[:], hsh^ovf.Seed)
	ovf.Rnd.Seed(seed)

	slots := len(ovf.Slots)

	// Random probing
	idx := int(hsh % uint32(slots))
	probes := int(ovf.Loglogn)
	if fullProbe {
		probes = slots
	}
	for i := 0; i < probes; i++ {
		if ovf.Slots[idx] == nil {
			ovf.Slots[idx] = newSlot(key, value)
			return true
		}
		idx = int(ovf.Rnd.Uint64() % uint64(slots))
	}

	return false
}

// overflowUniformLookup searches for a key-value pair in the overflow1 bank. This bank behaves as a separate
// open-addressed hash table with uniform random probing. Returns a found slot and true if the slot was found, otherwise
// nil and false. The fullProbe is true if the insertion must probe the whole table instead of the log(log(n)) slots.
func overflowUniformLookup(ovf *Overflow, hsh uint32, key []byte, fullProbe bool) (*Slot, bool) {
	var seed [32]byte
	binary.BigEndian.PutUint32(seed[:], hsh^ovf.Seed)
	ovf.Rnd.Seed(seed)

	slots := len(ovf.Slots)

	idx := int(hsh % uint32(slots))
	probes := int(ovf.Loglogn)
	if fullProbe {
		probes = slots
	}
	for i := 0; i < probes; i++ {
		if ovf.Slots[idx] == nil {
			return nil, false
		}
		if slices.Equal(ovf.Slots[idx].Key, key) {
			return ovf.Slots[idx], true
		}
		idx = int(ovf.Rnd.Uint64() % uint64(slots))
	}

	return nil, false
}

// overflowTwoChoiceInsert tries to insert a key-value pair into the overflow2 bank. This bank behaves as a separate
// open-addressed hash table with buckets and two-choice hashing.
// Returns a found slot and true if the slot was found, otherwise nil and false.
func overflowTwoChoiceInsert(ovf *Overflow, hsh1, hsh2 uint32, key []byte, value any) bool {
	// Linear probing two buckets, fail if both are full
	bucketSize := int(2 * ovf.Loglogn)
	buckets := len(ovf.Slots) / bucketSize
	bucket1 := int(hsh1%uint32(buckets)) * bucketSize
	bucket2 := int(hsh2%uint32(buckets)) * bucketSize
	for j := 0; j < bucketSize; j++ {
		if ovf.Slots[bucket1+j] == nil {
			ovf.Slots[bucket1+j] = newSlot(key, value)
			return true
		}
		if ovf.Slots[bucket2+j] == nil {
			ovf.Slots[bucket2+j] = newSlot(key, value)
			return true
		}
	}

	return false
}

// overflowTwoChoiceLookup searches for a key-value pair in the overflow2 bank. This bank behaves as a separate
// open-addressed hash table with buckets and two-choice hashing.
// Returns a found slot and true if the slot was found, otherwise nil and false.
func overflowTwoChoiceLookup(ovf *Overflow, hsh1, hsh2 uint32, key []byte) (*Slot, bool) {
	// Linear probing two buckets
	bucketSize := int(2 * ovf.Loglogn)
	buckets := len(ovf.Slots) / bucketSize
	bucket1 := int(hsh1%uint32(buckets)) * bucketSize
	bucket2 := int(hsh2%uint32(buckets)) * bucketSize
	for j := 0; j < bucketSize; j++ {
		if ovf.Slots[bucket1+j] == nil {
			return nil, false
		}
		if slices.Equal(ovf.Slots[bucket1+j].Key, key) {
			return ovf.Slots[bucket1+j], true
		}
		if ovf.Slots[bucket2+j] == nil {
			return nil, false
		}
		if slices.Equal(ovf.Slots[bucket2+j].Key, key) {
			return ovf.Slots[bucket2+j], true
		}
	}

	return nil, false
}

func newSlot(key []byte, value any) *Slot {
	return &Slot{
		Key:     key,
		Value:   value,
	}
}
