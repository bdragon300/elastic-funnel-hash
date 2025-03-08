package funnel

import (
	"encoding/binary"
	"math/rand/v2"
	"slices"
)

type Bbank struct {
	Slots []*bslot // Contains buckets * β slots
	Size  int
	Next  *Bbank // Ai+1 bank
}

type bslot struct {
	Tophash byte
	Key     []byte
	Value   any
}

type Boverflow struct {
	Slots   []*bslot
	Loglogn float64 // log2(log2(capacity))
	Seed    uint32
	Rnd     *rand.ChaCha8
}

func insert(table *HashTable, key []byte, value any) {
	hsh := table.Hasher(key)
	ok := bankInsert(table, table.Banks, hsh, key, value)
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

func lookup(table *HashTable, key []byte) (*bslot, bool) {
	hsh := table.Hasher(key)
	if value, ok := bankLookup(table, table.Banks, hsh, key); ok {
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
func bankInsert(table *HashTable, bank *Bbank, hsh uint32, key []byte, value any) bool {
	if bank == nil {
		return false
	}
	slots := bank.Size
	if bank.Slots == nil {
		bank.Slots = make([]*bslot, slots)
	}

	buckets := slots / table.BucketSize
	bucketOffset := int(hsh%uint32(buckets)) * table.BucketSize
	startOffset := int(hsh % uint32(table.BucketSize))

	// Linear circular probing, start from "random" slot in bucket
	for j := 0; j < table.BucketSize; j++ {
		idx := bucketOffset + (startOffset+j)%table.BucketSize
		if bank.Slots[idx] == nil {
			bank.Slots[idx] = newSlot(hsh, key, value)
			return true
		}
	}

	return bankInsert(table, bank.Next, hsh, key, value)
}

// bankLookup searches for a key-value pair in a banks except overflow banks.
func bankLookup(table *HashTable, bank *Bbank, hsh uint32, key []byte) (*bslot, bool) {
	if bank == nil {
		return nil, false
	}
	slots := len(bank.Slots)

	buckets := slots / table.BucketSize
	offset := int(hsh%uint32(buckets)) * table.BucketSize

	// Linear circular probing, start from "random" slot in bucket
	for j := 0; j < table.BucketSize; j++ {
		idx := offset + j%table.BucketSize
		if bank.Slots[idx] == nil {
			continue
		}
		if bank.Slots[idx].Tophash == tophash(hsh) && slices.Equal(bank.Slots[idx].Key, key) {
			return bank.Slots[idx], true
		}
	}

	return bankLookup(table, bank.Next, hsh, key)
}

// overflowUniformInsert tries to insert a key-value pair into the overflow1 bank. This bank behaves as a separate
// open-addressed hash table with uniform random probing. Returns true if the insertion was successful, otherwise false.
// The fullProbe is true if the insertion must probe the whole table instead of the log(log(n)) slots.
func overflowUniformInsert(ovf *Boverflow, hsh uint32, key []byte, value any, fullProbe bool) bool {
	var seed [32]byte
	binary.BigEndian.PutUint32(seed[:], hsh^ovf.Seed)
	ovf.Rnd.Seed(seed)

	slots := len(ovf.Slots)

	// Random probing
	idx := hsh % uint32(slots)
	probes := int(ovf.Loglogn)
	if fullProbe {
		probes = slots
	}
	for i := 0; i < probes; i++ {
		if ovf.Slots[idx] == nil {
			ovf.Slots[idx] = newSlot(hsh, key, value)
			return true
		}
		idx = uint32(ovf.Rnd.Uint64() % uint64(slots))
	}

	return false
}

// overflowUniformLookup searches for a key-value pair in the overflow1 bank. This bank behaves as a separate
// open-addressed hash table with uniform random probing. Returns a found slot and true if the slot was found, otherwise
// nil and false. The fullProbe is true if the insertion must probe the whole table instead of the log(log(n)) slots.
func overflowUniformLookup(ovf *Boverflow, hsh uint32, key []byte, fullProbe bool) (*bslot, bool) {
	var seed [32]byte
	binary.BigEndian.PutUint32(seed[:], hsh^ovf.Seed)
	ovf.Rnd.Seed(seed)

	slots := len(ovf.Slots)

	// Random probing
	idx := hsh % uint32(slots)
	probes := int(ovf.Loglogn)
	if fullProbe {
		probes = slots
	}
	for i := 0; i < probes; i++ {
		if ovf.Slots[idx] == nil {
			return nil, false
		}
		if ovf.Slots[idx].Tophash == tophash(hsh) && slices.Equal(ovf.Slots[idx].Key, key) {
			return ovf.Slots[idx], true
		}
		idx = uint32(ovf.Rnd.Uint64() % uint64(slots))
	}

	return nil, false
}

// overflowTwoChoiceInsert tries to insert a key-value pair into the overflow2 bank. This bank behaves as a separate
// open-addressed hash table with buckets and two-choice hashing.
// Returns a found slot and true if the slot was found, otherwise nil and false.
func overflowTwoChoiceInsert(ovf *Boverflow, hsh1, hsh2 uint32, key []byte, value any) bool {
	// Linear probing two buckets, fail if both are full
	bucketSize := uint32(2 * ovf.Loglogn)
	bidx1 := hsh1 % bucketSize
	bidx2 := hsh2 % bucketSize
	for j := uint32(0); j < bucketSize; j++ {
		if ovf.Slots[bidx1+j] == nil {
			ovf.Slots[bidx1+j] = newSlot(hsh1, key, value)
			return true
		}
		if ovf.Slots[bidx2+j] == nil {
			ovf.Slots[bidx2+j] = newSlot(hsh2, key, value)
			return true
		}
	}

	return false
}

// overflowTwoChoiceLookup searches for a key-value pair in the overflow2 bank. This bank behaves as a separate
// open-addressed hash table with buckets and two-choice hashing.
// Returns a found slot and true if the slot was found, otherwise nil and false.
func overflowTwoChoiceLookup(ovf *Boverflow, hsh1, hsh2 uint32, key []byte) (*bslot, bool) {
	// Linear probing two buckets
	bucketSize := uint32(2 * ovf.Loglogn)
	bidx1 := hsh1 % bucketSize
	bidx2 := hsh2 % bucketSize
	for j := uint32(0); j < bucketSize; j++ {
		if ovf.Slots[bidx1+j] == nil {
			return nil, false
		}
		if ovf.Slots[bidx1+j].Tophash == tophash(hsh1) && slices.Equal(ovf.Slots[bidx1+j].Key, key) {
			return ovf.Slots[bidx1+j], true
		}
		if ovf.Slots[bidx2+j] == nil {
			return nil, false
		}
		if ovf.Slots[bidx2+j].Tophash == tophash(hsh2) && slices.Equal(ovf.Slots[bidx2+j].Key, key) {
			return ovf.Slots[bidx2+j], true
		}
	}

	return nil, false
}

func newSlot(hsh uint32, key []byte, value any) *bslot {
	return &bslot{
		Tophash: tophash(hsh),
		Key:     key,
		Value:   value,
	}
}

func tophash(h uint32) byte {
	return byte(h >> 24)
}
