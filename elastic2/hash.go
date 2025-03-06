package elastic2

import (
	"hash/maphash"
	"math"
)

// TODO: go run -gcflags="-d=ssa/check_bce" example2.go

func NewHashTableDefault(capacity int) *HashTable {
	return NewHashTable(capacity, 0.1, 3/4, 200)
}

func NewHashTable(capacity int, delta, shrinkRatio, fillRate float64) *HashTable {
	// We use the power of 2 as bank size only for convenience. So they will have sizes, say, 16, 8, 4, 2, 1.
	// Actually, the size may have any base.
	banks := math.Ceil(math.Log2(float64(capacity)))
	capacity = int(math.Pow(2, banks))
	return &HashTable{
		Hasher:      defaultHasher(maphash.MakeSeed()),
		Prober:      defaultProber,
		fillRate:    fillRate,
		shrinkRatio: shrinkRatio,
		capacity:    capacity,
		delta:       delta,
		root: &bbank{
			slots: make([]*bslot, capacity),
		},
	}
}

type HashTable struct {
	Hasher      func(b []byte) uint32
	Prober      func(hsh, prevProbe uint32) uint32
	fillRate    float64
	shrinkRatio float64
	capacity    int
	inserts     int
	delta       float64
	root        *bbank
}

func (t *HashTable) Insert(key []byte, value any) {
	if t.inserts >= t.capacity {
		panic("capacity exceeded")
	}
	hsh := t.Hasher(key)
	insert(t, t.root, hsh, key, value)
}

func (t *HashTable) Set(key []byte, value any) bool {
	hsh := t.Hasher(key)
	slot, ok := lookup(t, t.root, hsh, key)
	if ok {
		slot.value = value
	} else {
		t.Insert(key, value)
	}
	return ok
}

func (t *HashTable) Get(key []byte) (any, bool) {
	hsh := t.Hasher(key)
	if slot, ok := lookup(t, t.root, hsh, key); ok {
		return slot.value, true
	}
	return nil, false
}

func (t *HashTable) Len() int {
	return t.inserts
}

func (t *HashTable) Capacity() int {
	return t.capacity
}

func defaultHasher(seed maphash.Seed) func(b []byte) uint32 {
	const prime32 = 4294967291
	return func(b []byte) uint32 {
		h := maphash.Bytes(seed, b)
		// fold 64-bit hash to 32-bit
		return uint32(h % prime32)
	}
}

func defaultProber(_, prevProbe uint32) uint32 {
	return prevProbe + 1
}
