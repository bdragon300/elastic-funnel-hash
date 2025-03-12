package funnel

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand/v2"
	"testing"
)

func TestOverflowTwoChoiceInsert(t *testing.T) {
	// 8 buckets, 4 slots each
	const (
		bucketSize = 4
		buckets    = 8
	)

	t.Run("insert and lookup; should return value by key", func(t *testing.T) {
		ovf := Overflow{Slots: make([]*Slot, bucketSize*buckets), Loglogn: bucketSize / 2}

		keys := []byte{4, 19, 33, 47}
		rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
		t.Logf("keys: %#v", keys)
		hashes1 := make([]uint32, bucketSize*buckets)
		hashes2 := make([]uint32, bucketSize*buckets)
		for i, k := range keys {
			hashes1[i] = uint32(k*k)
			hashes2[i] = uint32(k*k)
		}

		for i, k := range keys {
			assert.True(
				t, overflowTwoChoiceInsert(&ovf, hashes1[i], hashes2[i], []byte{k}, []byte{k}),
				"[%v]: %v, %v", i, hashes1[i], hashes2[i],
			)
		}

		for i, k := range keys {
			slot, ok := overflowTwoChoiceLookup(&ovf, hashes1[i], hashes2[i], []byte{k})
			assert.True(t, ok)
			assert.Equal(t, []byte{k}, slot.Key)
			assert.Equal(t, []byte{k}, slot.Value)
		}
	})

	t.Run("buckets to insert are full; should fail", func(t *testing.T) {
		slots := make([]*Slot, bucketSize*buckets)
		for i := bucketSize * 1; i < bucketSize*1+bucketSize; i++ { // Fill bucket 1
			slots[i] = &Slot{
				Key:     []byte{byte(i)},
				Value:   []byte{byte(i)},
			}
		}
		for i := bucketSize * 4; i < bucketSize*4+bucketSize; i++ { // Fill bucket 4
			slots[i] = &Slot{
				Key:     []byte{byte(i)},
				Value:   []byte{byte(i)},
			}
		}
		ovf := Overflow{Slots: slots, Loglogn: bucketSize / 2}

		hsh1 := uint32(8657) // bucket 1
		hsh2 := uint32(9812) // bucket 4

		assert.False(
			t, overflowTwoChoiceInsert(&ovf, hsh1, hsh2, []byte{0}, []byte{0}),
			"table overflow",
		)
	})
}

func TestOverflowTwoChoiceLookup(t *testing.T) {
	// 8 buckets, 4 slots each
	const (
		bucketSize = 4
		buckets    = 8
	)

	t.Run("lookup in full table; should return all keys", func(t *testing.T) {
		var slots []*Slot
		for i := 0; i < bucketSize*buckets; i++ {
			slots = append(slots, &Slot{
				Key:     []byte{byte(i)},
				Value:   []byte{byte(i)},
			})
		}
		ovf := Overflow{Slots: slots, Loglogn: bucketSize / 2}

		hsh1 := uint32(8663) // bucket 7
		hsh2 := uint32(9811) // bucket 3

		for i := uint32(7 * bucketSize); i < 7*bucketSize+bucketSize; i++ {
			slot, ok := overflowTwoChoiceLookup(&ovf, hsh1, hsh2, []byte{byte(i)})
			assert.True(t, ok)
			assert.Equal(t, slots[i], slot)
		}
		for i := uint32(3 * bucketSize); i < 3*bucketSize+bucketSize; i++ {
			slot, ok := overflowTwoChoiceLookup(&ovf, hsh1, hsh2, []byte{byte(i)})
			assert.True(t, ok)
			assert.Equal(t, slots[i], slot)
		}
	})

	t.Run("lookup wrong key or hash; should fail", func(t *testing.T) {
		var slots []*Slot
		for i := 0; i < bucketSize*buckets; i++ {
			slots = append(slots, &Slot{
				Key:     []byte{byte(i)},
				Value:   []byte{byte(i)},
			})
		}
		ovf := Overflow{Slots: slots, Loglogn: bucketSize / 2}

		hsh1 := uint32(8663) // bucket 7
		hsh2 := uint32(9811) // bucket 3

		// Hash matches, but key is different
		for i := uint32(7 * bucketSize); i < 7*bucketSize+bucketSize; i++ {
			_, ok := overflowTwoChoiceLookup(&ovf, hsh1, hsh2, []byte{byte(i + 100)})
			assert.False(t, ok)
		}
		for i := uint32(3 * bucketSize); i < 3*bucketSize+bucketSize; i++ {
			_, ok := overflowTwoChoiceLookup(&ovf, hsh1, hsh2, []byte{byte(i + 100)})
			assert.False(t, ok)
		}
		// Key matches, but hash is different
		for i := uint32(7 * bucketSize); i < 7*bucketSize+bucketSize; i++ {
			h1 := hsh1 +1
			h2:=hsh2+1
			_, ok := overflowTwoChoiceLookup(&ovf, h1, h2, []byte{byte(i)})
			assert.False(t, ok)
		}
		for i := uint32(3 * bucketSize); i < 3*bucketSize+bucketSize; i++ {
			h1 := hsh1 +1
			h2:=hsh2+1
			_, ok := overflowTwoChoiceLookup(&ovf, h1, h2, []byte{byte(i)})
			assert.False(t, ok)
		}
	})

	t.Run("lookup in empty table; should fail", func(t *testing.T) {
		// Ensure that the lookup function does not look outside a bucket that hash points to.
		ovf := Overflow{Slots: make([]*Slot, bucketSize*buckets), Loglogn: bucketSize / 2}

		hsh1 := uint32(8663) // bucket 7
		hsh2 := uint32(9811) // bucket 3

		for i := uint32(7 * bucketSize); i < 7*bucketSize+bucketSize; i++ {
			_, ok := overflowTwoChoiceLookup(&ovf, hsh1, hsh2, []byte{byte(i)})
			assert.False(t, ok)
		}
		for i := uint32(3 * bucketSize); i < 3*bucketSize+bucketSize; i++ {
			_, ok := overflowTwoChoiceLookup(&ovf, hsh1, hsh2, []byte{byte(i)})
			assert.False(t, ok)
		}
	})

	t.Run("lookup correct key in wrong buckets; should fail", func(t *testing.T) {
		// Ensure that the lookup function does not look outside a buckets pair that hashes point to.
		var slots []*Slot
		for i := 0; i < bucketSize*buckets; i++ {
			slots = append(slots, &Slot{
				Key:     []byte{byte(i)},
				Value:   []byte{byte(i)},
			})
		}
		ovf := Overflow{Slots: slots, Loglogn: bucketSize / 2}

		hsh1 := uint32(8662) // bucket 6
		hsh2 := uint32(9812) // bucket 4

		tests := []uint32{
			5 * bucketSize, 5*bucketSize + bucketSize - 1, // Keys are located in bucket 5
		}
		for _, tt := range tests {
			_, ok := overflowTwoChoiceLookup(&ovf, hsh1, hsh2, []byte{byte(tt)})
			assert.False(t, ok)
		}
	})
}

func TestOverflowUniformInsert(t *testing.T) {
	const (
		slotsCount = 32
		probeLimit = 3
		seed       = 1009
	)
	var rndSeed [32]byte
	binary.BigEndian.PutUint32(rndSeed[:], seed)

	t.Run("insert and lookup with limited probes; should be ok", func(t *testing.T) {
		rnd := rand.NewChaCha8(rndSeed)
		ovf := Overflow{Slots: make([]*Slot, slotsCount), Loglogn: probeLimit, Rnd: rnd, Seed: seed}
		keys := []byte{4, 19, 33, 47}
		rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
		t.Logf("keys: %#v", keys)
		hashes := make([]uint32, slotsCount)
		for i, k := range keys {
			hashes[i] = uint32(k*k)
		}

		for i, k := range keys {
			assert.True(
				t, overflowUniformInsert(&ovf, hashes[i], []byte{k}, []byte{k}, false),
				"[%v]: %v", i, hashes[i],
			)
		}

		for i, k := range keys {
			slot, ok := overflowUniformLookup(&ovf, hashes[i], []byte{k}, false)
			assert.True(t, ok)
			assert.Equal(t, []byte{k}, slot.Key)
			assert.Equal(t, []byte{k}, slot.Value)
		}
	})

	t.Run("insert and lookup will full probes; should be ok", func(t *testing.T) {
		rnd := rand.NewChaCha8(rndSeed)
		ovf := Overflow{Slots: make([]*Slot, slotsCount), Loglogn: probeLimit, Rnd: rnd, Seed: seed}
		keys := []byte{4, 19, 33, 47}
		rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
		t.Logf("keys: %#v", keys)
		hashes := make([]uint32, slotsCount)
		for i, k := range keys {
			hashes[i] = uint32(k*k)
		}

		for i, k := range keys {
			assert.True(
				t, overflowUniformInsert(&ovf, hashes[i], []byte{k}, []byte{k}, true),
				"[%v]: %v", i, hashes[i],
			)
		}

		for i, k := range keys {
			slot, ok := overflowUniformLookup(&ovf, hashes[i], []byte{k}, true)
			assert.True(t, ok)
			assert.Equal(t, []byte{k}, slot.Key)
			assert.Equal(t, []byte{k}, slot.Value)
		}
	})
}

func TestOverflowUniformLookup(t *testing.T) {
	const (
		probeLimit = 3
		seed       = 1009
	)
	var rndSeed [32]byte
	binary.BigEndian.PutUint32(rndSeed[:], seed)

	t.Run("lookup key before probe limit exceeds; should be ok", func(t *testing.T) {
		const slotsCount = 40
		ovf := Overflow{Slots: make([]*Slot, slotsCount), Loglogn: probeLimit}

		keys := []byte{4, 19, 33, 47}
		hashes := make([]uint32, slotsCount)
		for i, k := range keys {
			hashes[i] = uint32(k*k)
		}

		// Place items to the slots unreachable by the uniform probing
		for i, k := range keys {
			var s [32]byte
			binary.BigEndian.PutUint32(s[:], hashes[i]^seed)
			rnd := rand.NewChaCha8(s)

			idx := hashes[i] % slotsCount
			for p := 0; p < probeLimit-1; p++ {
				ovf.Slots[idx] = &Slot{} // Dummy item to keep the probes going
				idx = uint32(rnd.Uint64() % slotsCount)
			}
			require.Nil(t, ovf.Slots[idx], "[%v]: %v", idx, k) // Tune slotsCount or keys count if constantly fails
			ovf.Slots[idx] = &Slot{
				Key:     []byte{k},
				Value:   []byte{k},
			}
		}

		for i, k := range keys {
			ovf.Rnd = rand.NewChaCha8([32]byte{})
			ovf.Seed = seed
			slot, ok := overflowUniformLookup(&ovf, hashes[i], []byte{k}, false)
			assert.True(t, ok)
			assert.Equal(t, []byte{k}, slot.Key)
			assert.Equal(t, []byte{k}, slot.Value)
		}
	})

	t.Run("lookup key with probe limit exceeded; should fail", func(t *testing.T) {
		const slotsCount = 45
		ovf := Overflow{Slots: make([]*Slot, slotsCount), Loglogn: probeLimit}

		keys := []byte{5, 19, 33, 48}
		hashes := make([]uint32, slotsCount)
		for i, k := range keys {
			hashes[i] = uint32(k*k)
		}

		// Make items unreachable for random probing
		for i, k := range keys {
			var s [32]byte
			binary.BigEndian.PutUint32(s[:], hashes[i]^seed)
			rnd := rand.NewChaCha8(s)

			idx := hashes[i] % slotsCount
			for j := 0; j < probeLimit; j++ {
				ovf.Slots[idx] = &Slot{} // Dummy item to keep the probes going
				idx = uint32(rnd.Uint64() % slotsCount)
			}
			require.Nil(t, ovf.Slots[idx], "[%v]: %v", idx, k) // Tune slotsCount or keys count if constantly fails
			ovf.Slots[idx] = &Slot{
				Key:     []byte{k},
				Value:   []byte{k},
			}
		}

		for i, k := range keys {
			ovf.Rnd = rand.NewChaCha8([32]byte{})
			ovf.Seed = seed
			_, ok := overflowUniformLookup(&ovf, hashes[i], []byte{k}, false)
			assert.False(t, ok)
		}
	})
}

func TestBankInsert(t *testing.T) {
	// 8 banks with the following bucket counts of 4 slots
	const (
		bucketSize = 4
	)
	bucketCounts := []int{25, 19, 15, 11, 8, 6, 5, 4}

	t.Run("insert and lookup; should return value by key", func(t *testing.T) {
		banks := make([]*Bank, len(bucketCounts))
		var b *Bank
		for i := len(bucketCounts) - 1; i >= 0; i-- {
			banks[i] = &Bank{Data: make([]*Slot, bucketCounts[i]*bucketSize), Size: bucketCounts[i] * bucketSize, Next: b}
			b = banks[i]
		}

		keys := []byte{4, 19, 33, 47}
		rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
		t.Logf("keys: %#v", keys)
		var hashes []uint32
		for _, k := range keys {
			hashes = append(hashes, uint32(k*k))
		}

		for i, k := range keys {
			assert.True(
				t, bankInsert(banks[0], hashes[i], []byte{k}, []byte{k}, bucketSize),
				"[%v]: %v", i, hashes[i],
			)
		}

		for i, k := range keys {
			slot, ok := bankLookup(banks[0], hashes[i], []byte{k}, bucketSize)
			assert.True(t, ok)
			assert.Equal(t, []byte{k}, slot.Key)
			assert.Equal(t, []byte{k}, slot.Value)
		}
	})

	t.Run("buckets to insert are full; should fail", func(t *testing.T) {
		banks := make([]*Bank, len(bucketCounts))
		var b *Bank
		for i := len(bucketCounts) - 1; i >= 0; i-- {
			banks[i] = &Bank{Data: make([]*Slot, bucketCounts[i]*bucketSize), Size: bucketCounts[i] * bucketSize, Next: b}
			b = banks[i]
		}

		// Fully fill with dummy items only the buckets where the keys are going to be placed on insertion
		keys := []byte{4, 19, 33, 47}
		var hashes []uint32
		for _, k := range keys {
			hsh := uint32(k*k)
			hashes = append(hashes, hsh)
			for bank, count := range bucketCounts {
				bucket := int(hsh % uint32(count))
				for j := bucket * bucketSize; j < bucket*bucketSize+bucketSize; j++ {
					banks[bank].Data[j] = &Slot{}
				}
			}
		}

		for i, k := range keys {
			assert.False(t, bankInsert(banks[0], hashes[i], []byte{k}, []byte{k}, bucketSize))
		}
	})
}

func TestBankLookup(t *testing.T) {
	// 8 banks with the following bucket counts of 4 slots
	const (
		bucketSize = 4
	)
	bucketCounts := []int{25, 19, 15, 11, 8, 6, 5, 4}

	t.Run("lookup items placed in every bank; should be ok", func(t *testing.T) {
		banks := make([]*Bank, len(bucketCounts))
		var b *Bank
		for i := len(bucketCounts) - 1; i >= 0; i-- {
			banks[i] = &Bank{Data: make([]*Slot, bucketCounts[i]*bucketSize), Size: bucketCounts[i] * bucketSize, Next: b}
			b = banks[i]
		}

		// Put items to each bank to slot 0 of every bucket it should be placed
		keys := []byte{3, 37, 110}
		var hashes []uint32
		for _, k := range keys {
			hsh := uint32(k)
			hashes = append(hashes, hsh)
			for bank, size := range bucketCounts {
				bucket := int(hsh % uint32(size))
				require.Nil(t, banks[bank].Data[bucket*bucketSize], "[%v]: %v", bank, k) // Tune bucketsCounts or keys if constantly fails
				banks[bank].Data[bucket*bucketSize] = &Slot{
					Key:     []byte{k},
					Value:   []byte{k + byte(bank)}, // The result should come from the first bank, so value should be k
				}
			}
		}

		for i, k := range keys {
			slot, ok := bankLookup(banks[0], hashes[i], []byte{k}, bucketSize)
			assert.True(t, ok)
			assert.Equal(t, []byte{k}, slot.Key)
			assert.Equal(t, []byte{k}, slot.Value)
		}
	})

	t.Run("lookup items placed in the last bank; should be ok", func(t *testing.T) {
		banks := make([]*Bank, len(bucketCounts))
		var b *Bank
		for i := len(bucketCounts) - 1; i >= 0; i-- {
			banks[i] = &Bank{Data: make([]*Slot, bucketCounts[i]*bucketSize), Size: bucketCounts[i] * bucketSize, Next: b}
			b = banks[i]
		}

		// Put items to the last bank to slot 0 of bucket it should be placed
		keys := []byte{3, 37, 110}
		var hashes []uint32
		bank := len(bucketCounts) - 1
		for _, k := range keys {
			hsh :=uint32(k)
			hashes = append(hashes, hsh)
			bucket := int(hsh % uint32(bucketCounts[bank]))
			require.Nil(t, banks[bank].Data[bucket*bucketSize], "[%v]: %v", bank, k) // Tune bucketsCounts or keys if constantly fails
			banks[bank].Data[bucket*bucketSize] = &Slot{
				Key:     []byte{k},
				Value:   []byte{k},
			}
			bank0Bucket := int(hsh % uint32(bucketCounts[0]))
			banks[0].Data[bank0Bucket*bucketSize] = &Slot{} // Dummy item in bank 0 to make sure the lookup does not stop there
		}

		for i, k := range keys {
			slot, ok := bankLookup(banks[0], hashes[i], []byte{k}, bucketSize)
			assert.True(t, ok)
			assert.Equal(t, []byte{k}, slot.Key)
			assert.Equal(t, []byte{k}, slot.Value)
		}
	})

	t.Run("lookup in wrong buckets; should fail", func(t *testing.T) {
		banks := make([]*Bank, len(bucketCounts))
		var b *Bank
		for i := len(bucketCounts) - 1; i >= 0; i-- {
			banks[i] = &Bank{Data: make([]*Slot, bucketCounts[i]*bucketSize), Size: bucketCounts[i] * bucketSize, Next: b}
			b = banks[i]
		}

		// Put items to each bank to slot 0 of buckets it should not be placed
		keys := []byte{3, 37, 110}
		var hashes []uint32
		for _, k := range keys {
			hsh := uint32(k)
			hashes = append(hashes, hsh)
			for bank, size := range bucketCounts {
				bucket := int(hsh%uint32(size)) + 1
				if bucket > size-1 {
					bucket = 0
				}
				require.Nil(t, banks[bank].Data[bucket*bucketSize], "[%v]: %v", bank, k) // Tune bucketsCounts or keys if constantly fails
				banks[bank].Data[bucket*bucketSize] = &Slot{
					Key:     []byte{k},
					Value:   []byte{k + byte(bank)},
				}
			}
		}

		for i, k := range keys {
			_, ok := bankLookup(banks[0], hashes[i], []byte{k}, bucketSize)
			assert.False(t, ok)
		}
	})

	t.Run("lookup in empty table; should fail", func(t *testing.T) {
		banks := make([]*Bank, len(bucketCounts))
		var b *Bank
		for i := len(bucketCounts) - 1; i >= 0; i-- {
			banks[i] = &Bank{Data: make([]*Slot, bucketCounts[i]*bucketSize), Size: bucketCounts[i] * bucketSize, Next: b}
			b = banks[i]
		}

		// Put items to each bank to slot 0 of buckets it should not be placed
		keys := []byte{4, 19, 33, 47}
		var hashes []uint32
		for _, k := range keys {
			hsh := uint32(k*k)
			hashes = append(hashes, hsh)
		}

		for i, k := range keys {
			_, ok := bankLookup(banks[0], hashes[i], []byte{k}, bucketSize)
			assert.False(t, ok)
		}
	})
}
