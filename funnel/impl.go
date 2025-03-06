package funnel

import (
	"encoding/binary"
	"math"
	"math/rand/v2"
	"slices"
)

type bbank struct {
	slots []*bslot // Contains buckets * β slots
	next  *bbank   // Ai+1 bank
}

type bslot struct {
	tophash byte
	key     []byte
	value   any
}

type boverflow struct {
	slots   []*bslot
	loglogn float64 // log2(log2(capacity))
	seed    uint32
	rnd     *rand.ChaCha8
}

func insert(table *HashTable, key []byte, value any) {
	hsh := table.Hasher(key)
	if !bankInsert(table, table.root, hsh, key, value) {
		if !overflowUniformInsert(table.overflow1, hsh, key, value) {
			hsh = table.Hasher(key) ^ table.overflow1.seed
			hsh2 := table.Hasher(key) ^ table.overflow2.seed
			if !overflowTwoChoiceInsert(table.overflow2, hsh, hsh2, key, value) {
				panic("no free slot in overflow2 bucket")
			}
		}
	}
	table.inserts++
}

func lookup(table *HashTable, key []byte) (*bslot, bool) {
	hsh := table.Hasher(key)
	if value, ok := bankLookup(table, table.root, hsh, key); ok {
		return value, true
	}
	if value, ok := overflowUniformLookup(table.overflow1, hsh, key); ok {
		return value, true
	}
	hsh = table.Hasher(key) ^ table.overflow1.seed
	hsh2 := table.Hasher(key) ^ table.overflow2.seed
	return overflowTwoChoiceLookup(table.overflow2, hsh, hsh2, key)
}

// bankInsert makes "attempted insertion" a key-value pair into a banks except overflow banks.
func bankInsert(table *HashTable, bank *bbank, hsh uint32, key []byte, value any) bool {
	slotsLen := len(bank.slots)
	// Eliminate bounds check
	_ = bank.slots[slotsLen]

	buckets := uint32(slotsLen) / table.bucketSize
	offset := (hsh % buckets) * table.bucketSize
	startOffset := hsh % table.bucketSize

	// Linear circular probing, start from "random" slot in bucket
	for j := uint32(0); j < table.bucketSize; j++ {
		idx := offset + (startOffset+j)%table.bucketSize
		if bank.slots[idx] == nil {
			bank.slots[idx] = newSlot(hsh, key, value)
			return true
		}
	}

	if bank.next == nil {
		slots := uint32(math.Ceil(float64(slotsLen) * float64(table.shrinkRatio)))
		if slots < table.bucketSize {
			return false // This is the last bank
		}
		slots = slots + table.bucketSize - slots%table.bucketSize // Round up to the nearest multiple of β
		bank.next = &bbank{
			slots: make([]*bslot, slots),
		}
	}

	return bankInsert(table, bank.next, hsh, key, value)
}

// bankLookup searches for a key-value pair in a banks except overflow banks.
func bankLookup(table *HashTable, bank *bbank, hsh uint32, key []byte) (*bslot, bool) {
	slotsLen := len(bank.slots)
	// Eliminate bounds check
	_ = bank.slots[slotsLen]

	buckets := uint32(slotsLen) / table.bucketSize
	offset := (hsh % buckets) * table.bucketSize

	// Linear probing
	for j := offset; j < offset+table.bucketSize; j++ {
		if bank.slots[j] == nil {
			return nil, false
		}
		if bank.slots[j].tophash == tophash(hsh) && slices.Equal(bank.slots[j].key, key) {
			return bank.slots[j], true
		}
	}

	if bank.next == nil {
		return nil, false
	}

	return bankLookup(table, bank.next, hsh, key)
}

// overflowUniformInsert tries to insert a key-value pair into the overflow1 bank. This bank behaves as a separate
// open-addressed hash table with uniform random probing. Returns true if the insertion was successful, otherwise false.
func overflowUniformInsert(ovf *boverflow, hsh uint32, key []byte, value any) bool {
	var seed [32]byte
	binary.BigEndian.PutUint32(seed[:], hsh^ovf.seed)
	ovf.rnd.Seed(seed)

	slotsLen := len(ovf.slots)
	// Eliminate bounds check
	_ = ovf.slots[slotsLen]

	// Random probing
	idx := hsh % uint32(slotsLen)
	probes := int(ovf.loglogn)
	for i := 0; i < probes; i++ {
		if ovf.slots[idx] == nil {
			ovf.slots[idx] = newSlot(hsh, key, value)
			return true
		}
		idx = uint32(ovf.rnd.Uint64() % uint64(slotsLen))
	}

	return false
}

// overflowUniformLookup searches for a key-value pair in the overflow1 bank. This bank behaves as a separate
// open-addressed hash table with uniform random probing. Returns a found slot and true if the slot was found, otherwise
// nil and false.
func overflowUniformLookup(ovf *boverflow, hsh uint32, key []byte) (*bslot, bool) {
	var seed [32]byte
	binary.BigEndian.PutUint32(seed[:], hsh^ovf.seed)
	ovf.rnd.Seed(seed)

	slotsLen := len(ovf.slots)
	// Eliminate bounds check
	_ = ovf.slots[slotsLen]

	// Random probing
	idx := hsh % uint32(slotsLen)
	probes := int(ovf.loglogn)
	for i := 0; i < probes; i++ {
		if ovf.slots[idx] == nil {
			return nil, false
		}
		if ovf.slots[idx].tophash == tophash(hsh) && slices.Equal(ovf.slots[idx].key, key) {
			return ovf.slots[idx], true
		}
		idx = uint32(ovf.rnd.Uint64() % uint64(slotsLen))
	}

	return nil, false
}

// overflowTwoChoiceInsert tries to insert a key-value pair into the overflow2 bank. This bank behaves as a separate
// open-addressed hash table with buckets and two-choice hashing.
// Returns a found slot and true if the slot was found, otherwise nil and false.
func overflowTwoChoiceInsert(ovf *boverflow, hsh1, hsh2 uint32, key []byte, value any) bool {
	slotsLen := len(ovf.slots)
	// Eliminate bounds check
	_ = ovf.slots[slotsLen]

	// Linear probing two buckets, fail if both are full
	bucketSize := uint32(2 * ovf.loglogn)
	bidx1 := hsh1 % bucketSize
	bidx2 := hsh2 % bucketSize
	for j := uint32(0); j < bucketSize; j++ {
		if ovf.slots[bidx1+j] == nil {
			ovf.slots[bidx1+j] = newSlot(hsh1, key, value)
			return true
		}
		if ovf.slots[bidx2+j] == nil {
			ovf.slots[bidx2+j] = newSlot(hsh2, key, value)
			return true
		}
	}

	return false
}

// overflowTwoChoiceLookup searches for a key-value pair in the overflow2 bank. This bank behaves as a separate
// open-addressed hash table with buckets and two-choice hashing.
// Returns a found slot and true if the slot was found, otherwise nil and false.
func overflowTwoChoiceLookup(ovf *boverflow, hsh1, hsh2 uint32, key []byte) (*bslot, bool) {
	slotsLen := len(ovf.slots)
	// Eliminate bounds check
	_ = ovf.slots[slotsLen]

	// Linear probing two buckets
	bucketSize := uint32(2 * ovf.loglogn)
	bidx1 := hsh1 % bucketSize
	bidx2 := hsh2 % bucketSize
	for j := uint32(0); j < bucketSize; j++ {
		if ovf.slots[bidx1+j] == nil {
			return nil, false
		}
		if ovf.slots[bidx1+j].tophash == tophash(hsh1) && slices.Equal(ovf.slots[bidx1+j].key, key) {
			return ovf.slots[bidx1+j], true
		}
		if ovf.slots[bidx2+j] == nil {
			return nil, false
		}
		if ovf.slots[bidx2+j].tophash == tophash(hsh2) && slices.Equal(ovf.slots[bidx2+j].key, key) {
			return ovf.slots[bidx2+j], true
		}
	}

	return nil, false
}

func newSlot(hsh uint32, key []byte, value any) *bslot {
	return &bslot{
		tophash: tophash(hsh),
		key:     key,
		value:   value,
	}
}

func tophash(h uint32) byte {
	return byte(h >> 24)
}
