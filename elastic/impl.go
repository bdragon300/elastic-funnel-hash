package elastic

import (
	"math"
	"math/rand/v2"
	"slices"
)

type Bank struct {
	Data    []*Slot
	Inserts int
	Seed    [32]byte
}

type Slot struct {
	Key   []byte
	Value any
}

func insert(table *HashTable, hsh uint32, key []byte, value any) *Slot {
	// bankIndex points to Ai+1 bank, because according to the Paper, the insertion batch Bi goes to Ai+1 bank (B0 goes to A1, etc.)
	bankIndex := int(hsh % uint32(len(table.Banks)))
	bank := table.Banks[bankIndex] // Ai+1 bank
	epsilon2 := 1.0                // Ai+1 free slots fraction, 0..1
	if len(bank.Data) > 0 {
		epsilon2 = float64(len(bank.Data)-bank.Inserts) / float64(len(bank.Data))
	}

	if bankIndex == 0 {
		if epsilon2 <= 1-table.Bank2Occupation {
			return nil // No free slots
		}
		probes := len(bank.Data)
		offset := int(hsh % uint32(len(bank.Data)))
		return bankInsert(table, bank, key, value, offset, probes)
	}

	prevBank := table.Banks[bankIndex-1] // Ai bank
	epsilon1 := 1.0                      // Ai free slots fraction, 0..1
	if len(prevBank.Data) > 0 {
		epsilon1 = float64(len(prevBank.Data)-prevBank.Inserts) / float64(len(prevBank.Data))
	}

	switch {
	case epsilon1 <= table.Delta/2 && epsilon2 <= 1-table.Bank2Occupation:
		// The Paper states, that if epsilon1 ≤ δ/2 and epsilon2 ≤ 0.25 hold simultaneously, then batch Bi is over.
		return nil
	case epsilon1 <= table.Delta/2:
		// Case 2
		probes := len(bank.Data)
		offset := int(hsh % uint32(len(bank.Data)))
		return bankInsert(table, bank, key, value, offset, probes)
	case epsilon2 <= 1-table.Bank2Occupation:
		// Case 3
		probes := len(prevBank.Data)
		offset := int(hsh % uint32(len(prevBank.Data)))
		return bankInsert(table, prevBank, key, value, offset, probes)
	}

	// Case 1
	// epsilon1 > table.Delta/2 && epsilon2 > table.Bank2Occupation
	probes := int(table.Bank1FillFactor * min(math.Pow(math.Log2(1/epsilon1), 2), math.Log2(1/table.Delta)))
	probes = min(probes, len(prevBank.Data))
	offset := int(hsh % uint32(len(prevBank.Data)))
	slot := bankInsert(table, prevBank, key, value, offset, probes) // Ai bank
	if slot != nil {
		return slot
	}

	probes = len(bank.Data)
	offset = int(hsh % uint32(len(bank.Data)))
	return bankInsert(table, bank, key, value, offset, probes) // Ai+1 bank
}

func bankInsert(table *HashTable, bank *Bank, key []byte, value any, idx, probes int) *Slot {
	// Find the first free slot by random probing
	if probes == 0 {
		return nil
	}
	table.Rnd.Seed(bank.Seed)
	var j int
	for j = 0; j < probes && bank.Data[idx] != nil; j++ {
		idx = int(table.Rnd.Uint64() % uint64(len(bank.Data)))
	}
	if j == probes {
		return nil // No free slots
	}
	bank.Data[idx] = newSlot(key, value)
	bank.Inserts++
	table.Inserts++
	return bank.Data[idx]
}

func lookup(table *HashTable, hsh uint32, key []byte) (*Slot, bool) {
	// bankIndex points to Ai+1 bank, because according to the Paper, the insertion batch Bi goes to Ai+1 bank (B0 goes to A1, etc.)
	bankIndex := int(hsh % uint32(len(table.Banks)))
	bank := table.Banks[bankIndex] // Ai+1 bank
	if bankIndex == 0 {
		offset := int(hsh % uint32(len(bank.Data)))
		probes := len(bank.Data)
		table.Rnd.Seed(bank.Seed)
		if idx, ok := bankLookup(bank, key, offset, probes, table.Rnd); ok {
			return bank.Data[idx], true
		}
		return nil, false
	}

	epsilon1 := 1.0                      // Ai free slots fraction, 0..1
	prevBank := table.Banks[bankIndex-1] // Ai bank
	if len(prevBank.Data) > 0 {
		epsilon1 = float64(len(prevBank.Data)-prevBank.Inserts) / float64(len(prevBank.Data))
	}

	// Probe items from the most probable cases to the least probable, see the Paper pages 8-9
	// Limited probe the Ai bank (case 1)
	probes1 := int(table.Bank1FillFactor * min(math.Pow(math.Log2(1/epsilon1), 2), math.Log2(1/table.Delta)))
	probes1 = min(probes1, len(prevBank.Data))
	offset1 := int(hsh % uint32(len(prevBank.Data)))
	table.Rnd.Seed(prevBank.Seed)
	idx1, ok := bankLookup(prevBank, key, offset1, probes1, table.Rnd)
	if ok {
		return prevBank.Data[idx1], true
	}

	// Probe the Ai+1 bank (case 2)
	probes2 := len(bank.Data)
	offset2 := int(hsh % uint32(len(bank.Data)))
	table.Rnd2.Seed(bank.Seed)
	if idx, ok := bankLookup(bank, key, offset2, probes2, table.Rnd2); ok {
		return bank.Data[idx], true
	}

	// Resume probing the Ai bank (case 3)
	probes1 = len(prevBank.Data) - probes1
	if idx1, ok = bankLookup(prevBank, key, idx1, probes1, table.Rnd); ok {
		return prevBank.Data[idx1], true
	}
	return nil, false
}

// bankLookup searches for a key in the bank by random probing.
//
// Returns the index of the key and true if the key is found, or the next index to probe and false if the key is not found.
func bankLookup(bank *Bank, key []byte, idx, probes int, rnd *rand.ChaCha8) (int, bool) {
	// Random probing
	for j := 0; j < probes; j++ {
		if bank.Data[idx] == nil {
			continue
		}
		if slices.Equal(bank.Data[idx].Key, key) {
			return idx, true
		}
		idx = int(rnd.Uint64() % uint64(len(bank.Data)))
	}

	return idx, false
}

func newSlot(key []byte, value any) *Slot {
	return &Slot{
		Key:   key,
		Value: value,
	}
}
