package elastic1

import (
	"math"
	"slices"
)

type Bbank struct {
	Factor       int
	OriginalSize int
	FirstSlot    *Bslot
	LastSlot     *Bslot
	Next         *Bbank
}

type Bslot struct {
	Tophash byte
	Key     []byte
	Value   any
	Next    *Bslot
}

func insert(table *HashTable, key []byte, value any) {
	hsh := table.Hasher(key)

	bucket := getBucket(table, hsh)

	slot := insertBucket(table, key, value, table.Banks[bucket])
	if slot == nil {
		panic("table is full")
	}
	slot.Tophash = tophash(hsh)
}

func insertBucket(table *HashTable, key []byte, value any, bkt *Bbank) *Bslot {
	epsilon1 := float64(max(bkt.OriginalSize-bkt.Factor, 0)) / float64(bkt.OriginalSize) // Free slots fraction, 0..1

	// Next bucket epsilon
	epsilon2 := 1.0 // Free slots fraction, 0..1
	if bkt.Next != nil {
		epsilon2 = float64(min(bkt.Next.OriginalSize-bkt.Next.Factor, 0)) / float64(bkt.Next.Factor)
	}

	// Last slot to probe (included), slots after this are ignored
	var probes int
	switch {
	case bkt.Next == nil:
		return appendSlot(bkt, newSlot(key, value))
	case epsilon1 > table.Delta/2 && epsilon2 > table.BankShrink:
		// Probe only a portion of slots
		probes = int(table.BankOverflowFactor * min(math.Pow(math.Log2(1/epsilon1), 2), math.Log2(1/table.Delta)))
		probes = max(min(probes, bkt.Factor), 1) // At least one slot should be probed if bucket is free
	case epsilon1 <= table.Delta/2:
		probes = 0 // Go to the next bkt
	case epsilon2 <= table.BankShrink:
		table.Inserts++
		return appendSlot(bkt, newSlot(key, value))
	}

	if bkt.Factor < probes {
		table.Inserts++
		return appendSlot(bkt, newSlot(key, value))
	}

	return insertBucket(table, key, value, bkt.Next)
}

func appendSlot(bkt *Bbank, s *Bslot) *Bslot {
	if bkt.LastSlot != nil {
		bkt.LastSlot.Next = s
	}
	bkt.LastSlot = s
	if bkt.FirstSlot == nil {
		bkt.FirstSlot = s
	}
	bkt.Factor++
	return s
}

func lookup(table *HashTable, key []byte) (*Bslot, bool) {
	hsh := table.Hasher(key)
	bkt := getBucket(table, hsh)
	if slot, ok := lookupBucket(table, hsh, key, table.Banks[bkt]); ok {
		return slot, true
	}
	return nil, false
}

func lookupBucket(table *HashTable, hsh uint32, key []byte, bkt *Bbank) (*Bslot, bool) {
	// Linear probing
	for s := bkt.FirstSlot; s != nil; s = s.Next {
		if s.Tophash == tophash(hsh) && slices.Equal(s.Key, key) {
			return s, true
		}
	}
	if bkt.Next == nil {
		return nil, false
	}

	return lookupBucket(table, hsh, key, bkt.Next)
}

// getBucket returns a bucket index for the given hash.
// The returned bucket indexes has the logarithmic distribution in reverse direction.
func getBucket(table *HashTable, hsh uint32) int {
	buckets := len(table.Banks)
	slots := uint32(table.Capacity)
	offset := hsh % slots
	bktRev := math.Floor(math.Log2(float64(slots - offset))) // Bucket index counting from the end
	bkt := buckets - int(bktRev) - 1
	return bkt
}

func tophash(h uint32) byte {
	return byte(h >> 24)
}

func newSlot(key []byte, value any) *Bslot {
	return &Bslot{
		Key:   key,
		Value: value,
	}
}
