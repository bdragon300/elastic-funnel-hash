package elastic

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestInsert(t *testing.T) {
	const (
		capacity = 64 + 32 + 16 + 8 + 4 + 2 + 1
		seed     = 1009
	)
	banksCounts := []int{64, 32, 16, 8, 4, 2, 1}
	var rndSeed [32]byte
	binary.BigEndian.PutUint32(rndSeed[:], seed)

	t.Run("insert and lookup; should be ok", func(t *testing.T) {
		var banks []*Bank
		for _, size := range banksCounts {
			banks = append(banks, &Bank{Data: make([]*Slot, size), Seed: rndSeed})
		}
		table := HashTable{
			Bank1FillFactor: 200,
			Bank2Occupation: 0.75,
			Capacity:        capacity,
			Delta:           0.1,
			Banks:           banks,
			Rnd:             rand.NewChaCha8([32]byte{}),
			Rnd2:            rand.NewChaCha8([32]byte{}),
		}

		keys := []byte{7, 4, 19, 33, 47}
		//rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
		t.Logf("keys: %#v", keys)

		var hashes []uint32
		for _, k := range keys {
			hashes = append(hashes, uint32(k))
		}

		for i, k := range keys {
			slot := insert(&table, hashes[i], []byte{k}, []byte{k})
			assert.NotNil(t, slot)
			assert.Equal(t, []byte{k}, slot.Key)
			assert.Equal(t, []byte{k}, slot.Value)
		}

		for i, k := range keys {
			slot, ok := lookup(&table, hashes[i], []byte{k})
			assert.True(t, ok)
			assert.NotNil(t, slot)
			assert.Equal(t, []byte{k}, slot.Key)
			assert.Equal(t, []byte{k}, slot.Value)
		}
	})

	t.Run("insert to empty 1st bank; should be ok", func(t *testing.T) {
		var banks []*Bank
		for _, size := range banksCounts {
			banks = append(banks, &Bank{Data: make([]*Slot, size), Seed: rndSeed})
		}
		table := HashTable{
			Bank1FillFactor: 200,
			Bank2Occupation: 0.75,
			Capacity:        capacity,
			Delta:           0.1,
			Banks:           banks,
			Rnd:             rand.NewChaCha8([32]byte{}),
			Rnd2:            rand.NewChaCha8([32]byte{}),
		}

		key := byte(len(banks))

		expectData := make([]*Slot, len(banks[0].Data))
		hsh := uint32(key)
		expectData[hsh%uint32(len(banks[0].Data))] = &Slot{Key: []byte{key}, Value: []byte{key}}

		slot := insert(&table, hsh, []byte{key}, []byte{key})
		assert.NotNil(t, slot)
		assert.Equal(t, []byte{key}, slot.Key)
		assert.Equal(t, []byte{key}, slot.Value)

		assert.Equal(t, expectData, banks[0].Data)
		for bank := range banks[1:] {
			assert.Equal(t, make([]*Slot, len(banks[bank+1].Data)), banks[bank+1].Data)
		}
	})

	t.Run("insert to 1st bank, epsilon<=(1-bank2Occupation); should fail", func(t *testing.T) {
		var banks []*Bank
		for _, size := range banksCounts {
			banks = append(banks, &Bank{Data: make([]*Slot, size), Seed: rndSeed})
		}
		bank2Occupation := 0.75
		table := HashTable{
			Bank1FillFactor: 200,
			Bank2Occupation: bank2Occupation,
			Capacity:        capacity,
			Delta:           0.1,
			Banks:           banks,
			Rnd:             rand.NewChaCha8([32]byte{}),
			Rnd2:            rand.NewChaCha8([32]byte{}),
		}

		key := byte(len(banks))
		banks[0].Inserts = int(float64(len(banks[0].Data)) * bank2Occupation)

		hsh := uint32(key)

		slot := insert(&table, hsh, []byte{key}, []byte{key})
		assert.Nil(t, slot)

		for bank := range banks {
			assert.Equal(t, make([]*Slot, len(banks[bank].Data)), banks[bank].Data)
		}
	})

	t.Run("epsilon1==0, epsilon2>(1-bank2Occupation), empty bank1; should insert to bank2", func(t *testing.T) {
		var banks []*Bank
		for _, size := range banksCounts {
			banks = append(banks, &Bank{Data: make([]*Slot, size), Seed: rndSeed})
		}
		table := HashTable{
			Bank1FillFactor: 200,
			Bank2Occupation: 0.75,
			Capacity:        capacity,
			Delta:           0.1,
			Banks:           banks,
			Rnd:             rand.NewChaCha8([32]byte{}),
			Rnd2:            rand.NewChaCha8([32]byte{}),
		}

		key := byte(len(banks) + 1) // banks[1]

		expectData := make([]*Slot, len(banks[1].Data))
		hsh := uint32(key)
		expectData[hsh%uint32(len(banks[1].Data))] = &Slot{Key: []byte{key}, Value: []byte{key}}

		slot := insert(&table, hsh, []byte{key}, []byte{key})
		assert.NotNil(t, slot)
		assert.Equal(t, []byte{key}, slot.Key)
		assert.Equal(t, []byte{key}, slot.Value)

		assert.Equal(t, make([]*Slot, len(banks[0].Data)), banks[0].Data)
		assert.Equal(t, expectData, banks[1].Data)
		for bank := range banks[2:] {
			assert.Equal(t, make([]*Slot, len(banks[bank+2].Data)), banks[bank+2].Data)
		}
	})

	t.Run("epsilon1>delta/2, epsilon2>(1-bank2Occupation), non-empty bank1; should insert to bank1", func(t *testing.T) {
		var banks []*Bank
		for _, size := range banksCounts {
			banks = append(banks, &Bank{Data: make([]*Slot, size), Seed: rndSeed})
		}
		table := HashTable{
			Bank1FillFactor: 200,
			Bank2Occupation: 0.75,
			Capacity:        capacity,
			Delta:           0.1,
			Banks:           banks,
			Rnd:             rand.NewChaCha8([32]byte{}),
			Rnd2:            rand.NewChaCha8([32]byte{}),
		}

		key := byte(len(banks) + 1) // banks[1]
		banks[0].Inserts = 4

		expectData := make([]*Slot, len(banks[0].Data))
		hsh := uint32(key)
		expectData[hsh%uint32(len(banks[0].Data))] = &Slot{Key: []byte{key}, Value: []byte{key}}

		slot := insert(&table, hsh, []byte{key}, []byte{key})
		assert.NotNil(t, slot)
		assert.Equal(t, []byte{key}, slot.Key)
		assert.Equal(t, []byte{key}, slot.Value)

		assert.Equal(t, expectData, banks[0].Data)
		for bank := range banks[1:] {
			assert.Equal(t, make([]*Slot, len(banks[bank+1].Data)), banks[bank+1].Data)
		}
	})

	t.Run("epsilon1>delta/2, epsilon2>(1-bank2Occupation), several probes are failed in bank1; should insert to bank2", func(t *testing.T) {
		var banks []*Bank
		for _, size := range banksCounts {
			banks = append(banks, &Bank{Data: make([]*Slot, size), Seed: rndSeed})
		}
		fillFactor := 4.0
		delta := 0.1
		epsilon1 := 0.5
		probes := fillFactor * min(math.Pow(math.Log2(1/epsilon1), 2), math.Log2(1/delta))
		require.True(t, epsilon1 > delta/2)
		table := HashTable{
			Bank1FillFactor: fillFactor,
			Bank2Occupation: 0.75,
			Capacity:        capacity,
			Delta:           delta,
			Banks:           banks,
			Rnd:             rand.NewChaCha8([32]byte{}),
			Rnd2:            rand.NewChaCha8([32]byte{}),
		}

		banks[0].Inserts = int(probes + 1)
		var data []*Slot
		for i := 0; i < len(banks[0].Data); i++ {
			data = append(data, &Slot{}) // Dummy slot
		}
		banks[0].Data = slices.Clone(data)

		key := byte(len(banks) + 1) // banks[1]

		expectData := make([]*Slot, len(banks[1].Data))
		hsh := uint32(key)
		expectData[hsh%uint32(len(banks[1].Data))] = &Slot{Key: []byte{key}, Value: []byte{key}}

		slot := insert(&table, hsh, []byte{key}, []byte{key})
		assert.NotNil(t, slot)
		assert.Equal(t, []byte{key}, slot.Key)
		assert.Equal(t, []byte{key}, slot.Value)

		assert.Equal(t, data, banks[0].Data)
		assert.Equal(t, expectData, banks[1].Data)
		for bank := range banks[2:] {
			assert.Equal(t, make([]*Slot, len(banks[bank+2].Data)), banks[bank+2].Data)
		}
	})

	t.Run("epsilon1<=delta/2, epsilon2>(1-bank2Occupation); should insert to bank2", func(t *testing.T) {
		var banks []*Bank
		for _, size := range banksCounts {
			banks = append(banks, &Bank{Data: make([]*Slot, size), Seed: rndSeed})
		}
		delta := 0.1
		table := HashTable{
			Bank1FillFactor: 200,
			Bank2Occupation: 0.75,
			Capacity:        capacity,
			Delta:           delta,
			Banks:           banks,
			Rnd:             rand.NewChaCha8([32]byte{}),
			Rnd2:            rand.NewChaCha8([32]byte{}),
		}

		banks[0].Inserts = len(banks[0].Data) - int(float64(len(banks[0].Data))*(delta/2))

		key := byte(len(banks) + 1) // banks[1]

		expectData := make([]*Slot, len(banks[1].Data))
		hsh := uint32(key)
		expectData[hsh%uint32(len(banks[1].Data))] = &Slot{Key: []byte{key}, Value: []byte{key}}

		slot := insert(&table, hsh, []byte{key}, []byte{key})
		assert.NotNil(t, slot)
		assert.Equal(t, []byte{key}, slot.Key)
		assert.Equal(t, []byte{key}, slot.Value)

		assert.Equal(t, make([]*Slot, len(banks[0].Data)), banks[0].Data)
		assert.Equal(t, expectData, banks[1].Data)
		for bank := range banks[2:] {
			assert.Equal(t, make([]*Slot, len(banks[bank+2].Data)), banks[bank+2].Data)
		}
	})

	t.Run("epsilon1>delta/2, epsilon2<=(1-bank2Occupation); should insert to bank1", func(t *testing.T) {
		var banks []*Bank
		for _, size := range banksCounts {
			banks = append(banks, &Bank{Data: make([]*Slot, size), Seed: rndSeed})
		}
		delta := 0.1
		bank2Occupation := 0.75
		table := HashTable{
			Bank1FillFactor: 200,
			Bank2Occupation: bank2Occupation,
			Capacity:        capacity,
			Delta:           delta,
			Banks:           banks,
			Rnd:             rand.NewChaCha8([32]byte{}),
			Rnd2:            rand.NewChaCha8([32]byte{}),
		}

		banks[1].Inserts = int(float64(len(banks[1].Data)) * bank2Occupation)

		key := byte(len(banks) + 1) // banks[1]

		expectData := make([]*Slot, len(banks[0].Data))
		hsh := uint32(key)
		expectData[hsh%uint32(len(banks[0].Data))] = &Slot{Key: []byte{key}, Value: []byte{key}}

		slot := insert(&table, hsh, []byte{key}, []byte{key})
		assert.NotNil(t, slot)
		assert.Equal(t, []byte{key}, slot.Key)
		assert.Equal(t, []byte{key}, slot.Value)

		assert.Equal(t, expectData, banks[0].Data)
		for bank := range banks[1:] {
			assert.Equal(t, make([]*Slot, len(banks[bank+1].Data)), banks[bank+1].Data)
		}
	})

	t.Run("epsilon1<=delta/2, epsilon2<=(1-bank2Occupation); should fail", func(t *testing.T) {
		var banks []*Bank
		for _, size := range banksCounts {
			banks = append(banks, &Bank{Data: make([]*Slot, size), Seed: rndSeed})
		}
		delta := 0.1
		bank2Occupation := 0.75
		table := HashTable{
			Bank1FillFactor: 200,
			Bank2Occupation: bank2Occupation,
			Capacity:        capacity,
			Delta:           delta,
			Banks:           banks,
			Rnd:             rand.NewChaCha8([32]byte{}),
			Rnd2:            rand.NewChaCha8([32]byte{}),
		}

		banks[0].Inserts = len(banks[0].Data) - int(float64(len(banks[0].Data))*(delta/2))
		banks[1].Inserts = int(float64(len(banks[1].Data)) * bank2Occupation)

		key := byte(len(banks) + 1) // banks[1]
		hsh := uint32(key)

		slot := insert(&table, hsh, []byte{key}, []byte{key})
		assert.Nil(t, slot)

		for bank := range banks {
			assert.Equal(t, make([]*Slot, len(banks[bank].Data)), banks[bank].Data)
		}
	})

	t.Run("epsilon1<=delta/2, epsilon2>(1-bank2Occupation), bank1 is full; should insert to bank2 with probing a free slot", func(t *testing.T) {
		var banks []*Bank
		for _, size := range banksCounts {
			banks = append(banks, &Bank{Data: make([]*Slot, size), Seed: rndSeed})
		}
		delta := 0.1
		bank2Occupation := 0.75
		table := HashTable{
			Bank1FillFactor: 200,
			Bank2Occupation: bank2Occupation,
			Capacity:        capacity,
			Delta:           delta,
			Banks:           banks,
			Rnd:             rand.NewChaCha8([32]byte{}),
			Rnd2:            rand.NewChaCha8([32]byte{}),
		}

		banks[0].Inserts = len(banks[0].Data) - int(float64(len(banks[0].Data))*(delta/2))
		banks[1].Inserts = int(float64(len(banks[1].Data))*bank2Occupation) - 1

		var data0 []*Slot
		for i := 0; i < len(banks[0].Data); i++ {
			data0 = append(data0, &Slot{}) // Dummy slot
		}
		banks[0].Data = slices.Clone(data0)

		key := byte(len(banks) + 1) // banks[1]
		hsh := uint32(key)

		rnd := rand.NewChaCha8(rndSeed)
		data1 := make([]*Slot, len(banks[1].Data))
		idx := int(hsh % uint32(len(banks[1].Data)))
		for i := 0; i < banks[1].Inserts; i++ {
			data1[idx] = &Slot{} // Dummy slot
			idx = int(rnd.Uint64() % uint64(len(banks[1].Data)))
		}
		banks[1].Data = slices.Clone(data1)

		expectData := slices.Clone(data1)
		expectData[idx] = &Slot{Key: []byte{key}, Value: []byte{key}}

		slot := insert(&table, hsh, []byte{key}, []byte{key})
		assert.NotNil(t, slot)
		assert.Equal(t, []byte{key}, slot.Key)
		assert.Equal(t, []byte{key}, slot.Value)

		assert.Equal(t, data0, banks[0].Data)
		assert.Equal(t, expectData, banks[1].Data)
		for bank := range banks[2:] {
			assert.Equal(t, make([]*Slot, len(banks[bank+2].Data)), banks[bank+2].Data)
		}
	})

	t.Run("table is full; should fail", func(t *testing.T) {
		for tbank := range banksCounts {
			t.Run(fmt.Sprintf("bank %d", tbank), func(t *testing.T) {

				var banks []*Bank
				for _, size := range banksCounts {
					banks = append(banks, &Bank{Data: make([]*Slot, size), Seed: rndSeed})
				}
				table := HashTable{
					Bank1FillFactor: 200,
					Bank2Occupation: 0.75,
					Capacity:        capacity,
					Delta:           0.1,
					Banks:           banks,
					Rnd:             rand.NewChaCha8([32]byte{}),
					Rnd2:            rand.NewChaCha8([32]byte{}),
				}

				for bank, size := range banksCounts {
					for i := 0; i < size; i++ {
						banks[bank].Data[i] = &Slot{} // Dummy slot
					}
					banks[bank].Inserts = size
				}

				key := byte(len(banks) + tbank) // banks[1]
				hsh := uint32(key)

				slot := insert(&table, hsh, []byte{key}, []byte{key})
				assert.Nil(t, slot)
			})
		}
	})
}

func TestLookup(t *testing.T) {
	const (
		capacity = 64 + 32 + 16 + 8 + 4 + 2 + 1
		seed     = 1009
	)
	banksCounts := []int{64, 32, 16, 8, 4, 2, 1}
	var rndSeed [32]byte
	binary.BigEndian.PutUint32(rndSeed[:], seed)

	t.Run("put element to hash position and lookup; should be ok", func(t *testing.T) {
		// Place a slot to each bank to hashed position and try to lookup it
		for tbank := range banksCounts {
			t.Run(fmt.Sprintf("bank %d", tbank), func(t *testing.T) {
				var banks []*Bank
				for _, size := range banksCounts {
					banks = append(banks, &Bank{Data: make([]*Slot, size), Seed: rndSeed})
				}
				table := HashTable{
					Bank1FillFactor: 200,
					Bank2Occupation: 0.75,
					Capacity:        capacity,
					Delta:           0.1,
					Banks:           banks,
					Rnd:             rand.NewChaCha8([32]byte{}),
					Rnd2:            rand.NewChaCha8([32]byte{}),
				}

				for bank, size := range banksCounts {
					for i := 0; i < size; i++ {
						banks[bank].Data[i] = &Slot{} // Dummy slot
					}
					banks[bank].Inserts = size
				}

				key := byte(len(banks) + tbank) // banks[1]
				hsh := uint32(key)
				banks[tbank].Data[hsh%uint32(len(banks[tbank].Data))] = &Slot{Key: []byte{key}, Value: []byte{key}}

				slot, ok := lookup(&table, hsh, []byte{key})
				assert.True(t, ok)
				assert.NotNil(t, slot)
				assert.Equal(t, []byte{key}, slot.Key)
				assert.Equal(t, []byte{key}, slot.Value)
			})
		}
	})

	t.Run("lookup with probing; should be ok", func(t *testing.T) {
		// Place a slot to each bank to any position other than hashed and try to lookup it
		for tbank := range banksCounts {
			t.Run(fmt.Sprintf("bank %d", tbank), func(t *testing.T) {
				var banks []*Bank
				for _, size := range banksCounts {
					banks = append(banks, &Bank{Data: make([]*Slot, size), Seed: rndSeed})
				}
				table := HashTable{
					Bank1FillFactor: 200,
					Bank2Occupation: 0.75,
					Capacity:        capacity,
					Delta:           0.1,
					Banks:           banks,
					Rnd:             rand.NewChaCha8([32]byte{}),
					Rnd2:            rand.NewChaCha8([32]byte{}),
				}

				for bank, size := range banksCounts {
					for i := 0; i < size; i++ {
						banks[bank].Data[i] = &Slot{} // Dummy slot
					}
					banks[bank].Inserts = size
				}

				key := byte(len(banks) + tbank) // banks[tbank]

				hsh := uint32(key)
				rnd := rand.NewChaCha8(rndSeed)
				data1 := make([]*Slot, len(banks[tbank].Data))
				idx := int(hsh % uint32(len(banks[tbank].Data)))
				for i := 0; i < len(banks[tbank].Data)-2; i++ {
					data1[idx] = &Slot{} // Dummy slot
					idx = int(rnd.Uint64() % uint64(len(banks[tbank].Data)))
				}
				banks[tbank].Data[idx] = &Slot{Key: []byte{key}, Value: []byte{key}}

				slot, ok := lookup(&table, hsh, []byte{key})
				assert.True(t, ok)
				assert.NotNil(t, slot)
				assert.Equal(t, []byte{key}, slot.Key)
				assert.Equal(t, []byte{key}, slot.Value)
			})
		}
	})

	t.Run("lookup missed key in full table; should fail", func(t *testing.T) {
		for tbank := range banksCounts {
			t.Run(fmt.Sprintf("bank %d", tbank), func(t *testing.T) {
				var banks []*Bank
				for _, size := range banksCounts {
					banks = append(banks, &Bank{Data: make([]*Slot, size), Seed: rndSeed})
				}
				table := HashTable{
					Bank1FillFactor: 200,
					Bank2Occupation: 0.75,
					Capacity:        capacity,
					Delta:           0.1,
					Banks:           banks,
					Rnd:             rand.NewChaCha8([32]byte{}),
					Rnd2:            rand.NewChaCha8([32]byte{}),
				}

				for bank, size := range banksCounts {
					for i := 0; i < size; i++ {
						banks[bank].Data[i] = &Slot{} // Dummy slot
					}
					banks[bank].Inserts = size
				}

				key := byte(len(banks) + tbank) // banks[tbank]
				hsh := uint32(key)

				_, ok := lookup(&table, hsh, []byte{key})
				assert.False(t, ok)
			})
		}
	})

	t.Run("lookup missed key in empty table; should fail", func(t *testing.T) {
		for tbank := range banksCounts {
			t.Run(fmt.Sprintf("bank %d", tbank), func(t *testing.T) {
				var banks []*Bank
				for _, size := range banksCounts {
					banks = append(banks, &Bank{Data: make([]*Slot, size), Seed: rndSeed})
				}
				table := HashTable{
					Bank1FillFactor: 200,
					Bank2Occupation: 0.75,
					Capacity:        capacity,
					Delta:           0.1,
					Banks:           banks,
					Rnd:             rand.NewChaCha8([32]byte{}),
					Rnd2:            rand.NewChaCha8([32]byte{}),
				}

				key := byte(len(banks) + tbank) // banks[tbank]
				hsh := uint32(key)

				_, ok := lookup(&table, hsh, []byte{key})
				assert.False(t, ok)
			})
		}
	})
}
