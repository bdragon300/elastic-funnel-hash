package elastic

import (
	"math"
	"slices"
)

type bslot struct {
	tophash byte
	key     []byte
	value   any
}

func insert(table *HashTable, key []byte, value any) {
	hsh := table.Hasher(key) % uint32(len(table.slots))

	slot := insertBank(table, key, value, hsh, uint32(len(table.slots)))
	if slot == nil {
		// All banks >=bank are full, insert to bank #0 as the last resort
		slot = insertBank(table, key, value, 0, hsh)
	}
	if slot == nil {
		panic("table is full")
	}
	slot.tophash = tophash(hsh)
}

func insertBank(table *HashTable, key []byte, value any, offset, limitOffset uint32) *bslot {
	bankIndexRev := math.Floor(math.Log2(float64(uint32(len(table.slots)) - offset))) // Bank index counting from the end

	banks := len(table.inserts)                             // Banks count
	bank := banks - int(bankIndexRev) - 1                   // Current bank index from the start
	bankSize := uint32(math.Pow(2, bankIndexRev))           // Current bank size
	bankOffset := uint32(len(table.slots)) - bankSize*2 + 1 // Current bank offset

	if bankOffset+bankSize > limitOffset {
		return nil // We've reached the limit
	}

	epsilon1 := float64(bankSize-uint32(table.inserts[bank])) / float64(bankSize) // Free slots fraction, 0..1

	// Next bank epsilon
	epsilon2 := 1.0 // Next bank free slots fraction, 0..1
	var bankSize2 uint32
	if bank < banks-1 {
		bankSize2 = bankSize / 2
		epsilon2 = float64(bankSize2-uint32(table.inserts[bank+1])) / float64(bankSize2)
	}

	// Denotes the last slot to probe (included), slots after this are ignored
	var probeSize uint32
	switch {
	case epsilon1 > table.fullness/2 && epsilon2 > table.shrinkRatio:
		// Probe only a portion of slots
		probeSize = uint32(table.fillRate * min(math.Pow(math.Log2(1/epsilon1), 2), math.Log2(1/table.fullness)))
	case epsilon1 <= table.fullness/2:
		probeSize = 0 // Go to the next bank
	case epsilon2 <= table.shrinkRatio:
		probeSize = bankSize // Probe all slots
	}

	_ = table.slots[bankOffset+probeSize] // Eliminate bounds check
	if probeSize > 0 {
		// Linear circular probing slots on range bankOffset..bankOffset+bankSize
		for j := uint32(0); j < probeSize; j++ {
			idx := (bankOffset + j) % probeSize
			if table.slots[idx] == nil {
				table.slots[idx] = newSlot(key, value)
				table.inserts[bank]++
				table.totalInserts++
				return table.slots[idx]
			}
		}
	}

	if bankIndexRev == 0 {
		return nil
	}

	// Making next offset to depend on current offset (and eventually, on hash) could help mitigate primary-clustering problem
	nextOffset := bankOffset + bankSize + (offset % bankSize2)
	return insertBank(table, key, value, nextOffset, limitOffset)
}

func lookup(table *HashTable, key []byte) (*bslot, bool) {
	hsh := table.Hasher(key) % uint32(len(table.slots))
	if slot, ok := lookupBank(table, hsh, key, hsh); ok {
		return slot, true
	}
	if slot, ok := lookupBank(table, hsh, key, 0); ok {
		return slot, true
	}
	return nil, false
}

func lookupBank(table *HashTable, hsh uint32, key []byte, offset uint32) (*bslot, bool) {
	bankEndIndex := math.Floor(math.Log2(float64(uint32(len(table.slots)) - offset))) // Bank index from the end

	bankSize := uint32(math.Pow(2, bankEndIndex))
	bankOffset := uint32(len(table.slots)) - bankSize*2 + 1

	// Linear circular probing slots on range bankOffset..bankOffset+bankSize
	for j := uint32(0); j < bankSize; j++ {
		idx := (bankOffset + j) % bankSize
		if table.slots[idx] == nil {
			continue
		}
		if table.slots[idx].tophash == tophash(hsh) && slices.Equal(table.slots[idx].key, key) {
			return table.slots[idx], true
		}
	}
	if bankEndIndex == 0 {
		return nil, false
	}

	nextOffset := bankOffset + (offset % bankSize / 2)
	return lookupBank(table, hsh, key, nextOffset)
}

func tophash(h uint32) byte {
	return byte(h >> 24)
}

func newSlot(key []byte, value any) *bslot {
	return &bslot{
		key:   key,
		value: value,
	}
}
