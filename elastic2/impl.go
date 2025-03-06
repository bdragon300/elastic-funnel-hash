package elastic2

import (
	"math"
	"slices"
)

type bbank struct {
	slots   []*bslot
	inserts int
	next    *bbank
}

type bslot struct {
	tophash byte
	key     []byte
	value   any
}

func insert(table *HashTable, bank *bbank, hsh uint32, key []byte, value any) *bslot {
	epsilon1 := float64(len(bank.slots)-bank.inserts) / float64(len(bank.slots)) // Free slots fraction, 0..1

	// Next bank epsilon
	epsilon2 := 1.0 // Free slots fraction, 0..1
	if bank.next != nil {
		epsilon2 = float64(len(bank.next.slots)-bank.next.inserts) / float64(len(bank.next.slots))
	}

	slotsLen := len(bank.slots)
	offset := hsh % uint32(slotsLen)

	// The last slot to probe (included), slots after this are ignored
	var last uint32
	switch {
	case epsilon1 > table.fullness/2 && epsilon2 > table.shrinkRatio:
		// Probe only a portion of slots
		last = uint32(table.fillRate * min(math.Pow(math.Log2(1/epsilon1), 2), math.Log2(1/table.fullness)))
	case epsilon1 <= table.fullness/2:
		last = 0 // Go to the next bank
	case epsilon2 <= table.shrinkRatio:
		last = uint32(slotsLen) // Probe all slots
	}

	if last > 0 {
		_ = bank.slots[offset+last] // Eliminate bounds check
		// Linear circular probing slots on range 0..last
		for j := uint32(0); j < last; j++ {
			idx := (offset + j) % last
			if bank.slots[idx] == nil {
				bank.slots[idx] = newSlot(hsh, key, value)
				bank.inserts++
				table.inserts++
				return bank.slots[idx]
			}
		}
	}

	if bank.next == nil {
		slots := len(bank.slots) / 2
		if slots == 0 {
			panic("no free space")
		}
		bank.next = &bbank{
			slots: make([]*bslot, slots),
		}
	}

	return insert(table, bank.next, hsh, key, value)
}

func lookup(table *HashTable, bank *bbank, hsh uint32, key []byte) (*bslot, bool) {
	slotsLen := len(bank.slots)
	_ = bank.slots[slotsLen] // Eliminate bounds check

	offset := hsh % uint32(len(bank.slots))
	// Linear circular probing slots on range 0..last
	for j := uint32(0); j < uint32(slotsLen); j++ {
		idx := (offset + j) % uint32(slotsLen)
		if bank.slots[idx] == nil {
			return nil, false
		}
		if bank.slots[idx].tophash == tophash(hsh) && slices.Equal(bank.slots[idx].key, key) {
			return bank.slots[idx], false
		}
	}

	if bank.next == nil {
		return nil, false
	}

	return lookup(table, bank.next, hsh, key)
}

func tophash(h uint32) byte {
	return byte(h >> 24)
}

func newSlot(hsh uint32, key []byte, value any) *bslot {
	return &bslot{
		tophash: tophash(hsh),
		key:     key,
		value:   value,
	}
}
