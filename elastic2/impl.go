package elastic2

import (
	"math"
	"slices"
)

type Bbank struct {
	Slots   []*Bslot
	Inserts int
	Next    *Bbank
}

type Bslot struct {
	Tophash byte
	Key     []byte
	Value   any
}

func insert(table *HashTable, bank *Bbank, hsh uint32, key []byte, value any) *Bslot {
	epsilon1 := float64(len(bank.Slots)-bank.Inserts) / float64(len(bank.Slots)) // Free slots fraction, 0..1

	// Next bank epsilon
	epsilon2 := 1.0 // Free slots fraction, 0..1
	if bank.Next != nil {
		epsilon2 = float64(len(bank.Next.Slots)-bank.Next.Inserts) / float64(len(bank.Next.Slots))
	}

	slots := len(bank.Slots)

	// The last slot to probe (included), slots after this are ignored
	var probes uint32
	switch {
	case epsilon1 > table.Delta/2 && epsilon2 > table.BankShrink:
		// Probe only a portion of slots
		probes = uint32(table.OverflowFactor * min(math.Pow(math.Log2(1/epsilon1), 2), math.Log2(1/table.Delta)))
		probes = max(min(probes, uint32(slots)), 1) // At least one slot should be probed if bucket is free
	case epsilon1 <= table.Delta/2:
		probes = 0 // Go to the next bank
	case epsilon2 <= table.BankShrink:
		probes = uint32(slots) // Probe all slots
	}

	offset := hsh % uint32(slots)
	if probes > 0 {
		// Linear circular probing slots on range 0..probes
		for j := uint32(0); j < probes; j++ {
			idx := (offset + j) % probes
			if bank.Slots[idx] == nil {
				bank.Slots[idx] = newSlot(key, value)
				bank.Inserts++
				table.Inserts++
				return bank.Slots[idx]
			}
		}
	}

	if bank.Next == nil {
		slots := len(bank.Slots) / 2
		if slots == 0 {
			panic("no free space")
		}
		bank.Next = &Bbank{
			Slots: make([]*Bslot, slots),
		}
	}

	return insert(table, bank.Next, hsh, key, value)
}

func lookup(table *HashTable, bank *Bbank, hsh uint32, key []byte) (*Bslot, bool) {
	slots := uint32(len(bank.Slots))

	offset := hsh % slots
	// Linear circular probing
	for j := uint32(0); j < slots; j++ {
		idx := (offset + j) % slots
		if bank.Slots[idx] == nil {
			continue
		}
		if bank.Slots[idx].Tophash == tophash(hsh) && slices.Equal(bank.Slots[idx].Key, key) {
			return bank.Slots[idx], true
		}
	}

	if bank.Next == nil {
		return nil, false
	}

	return lookup(table, bank.Next, hsh, key)
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
