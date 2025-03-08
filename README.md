# Elastic/Funnel hashing PoC

This is a PoC of funnel hashing and elastic hashing in Go.

The implementation based on the paper [Optimal Bounds for Open Addressing Without Reordering](https://arxiv.org/abs/2501.02305) by Martin Farach-Colton, Andrew Krapivin, William Kuszmaul

## Installation

```bash
go get github.com/bdragon300/elastic-funnel-hash
```

## Usage

```go
package main

import (
    "fmt"
    "github.com/bdragon300/elastic-funnel-hashing/funnel"
)

func main() {
	h := funnel.NewHashTableDefault(100)
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("key%d", i)
		h.Insert([]byte(k), []byte(fmt.Sprintf("value%d", i)))
	}
	v, ok := h.Get([]byte("key0"))
	fmt.Printf("%s, %v", v, ok)
}
```

## Elastic hashing

Basically, this is the hash table with open addressing, where the data is stored in data banks of geometrically
decreasing sizes. The lookup/insert process depends on each data bank fullness.

This PoC contains two variants of algorithm, because its description in the paper mentioned above may be interpreted 
in different ways (especially for the lookup).

### Variant 1

In this implementation, banks count are calculated on creation and fixed. Table size is not limited, since the banks 
has dynamic size. Every bank is the linked list of slots.

All data slots are divided into banks with dynamic size geometrically decreasing by the power of 2. 
Every slot stores a key, value, and the first byte of hash to speed up the key probing.

In this implementation, the geometric decreasing rate fully depends on hash function quality, specifically on 
its even distribution of values, because the bank index is calculated roughly as `log2(hash)`.
Therefore, if the hash function is uniform, banks obey to the geometric progression `2^i`.

To make the insert or lookup, first we select a bank based on the key hash value.
On collision, we do the linear probing starting from the first bank slot.
For inserts, based on selected bank metrics we decide whether we should insert the item to this bank
or the next one. If we choose the next bank, we do the same for it, and so on, until the item is inserted.

### Variant 2

In this implementation, banks count are calculated on creation and fixed.
Table size is also fixed and can be set on creation.

All data slots are divided into banks with fixed size geometrically decreasing by the power of 2. Every slot stores
a key, value, and the first byte of hash to speed up the key probing.

Inserts and lookups are always start from the 1st bank. On collision, we do the circular linear probing staring
from offset calculated from the hash. Once all slots are probed, we move to the next bank.
For inserts, based on bank metrics we decide whether we should insert the item to this bank
or the next one. If we choose the next bank, we do the same for it, and so on, until the item is inserted or we
reach the end.

## Funnel hashing

This is the hash table with open addressing, where the data is stored in data banks of geometrically
decreasing sizes. Unlike the elastic hashing, the banks are additionally divided by buckets, and the lookup/insert 
process is just linear. Also, this algorithm uses two additional mini-hashtables.

Basically, all data slots are divided into three unequal parts:

1. 90-95% of data is stored in fixed count of banks, each consists of fixed size buckets. The number of buckets
   in every bank geometrically decreases from table start towards the end. Bucket size, bucket
   count and banks count are calculated upon table creation.
2. Overflow1 bucket, that actually is another separate mini-hashtable supporting the uniform random probing.
   May occupy up to 5% of the table.
3. Overflow2 bucket, that is a separate mini-hashtable supporting the two-choice hashing containing the fixed size buckets.
   May occupy up to 5% of the table.

Inserts and lookups always start from the 1st bank. We probe only one bucket in every bank selected based on the key hash.
If probing fails (bucket is full on insert or doesn't contain a key we're looking for on lookup), we hop to the next bank.
Once the last bank is reached, we start to process the overflow1 bucket. If it fails, we continue with the
overflow2 bucket. If the overflow2 bucket fails, the process is failed.

Overflow2 bucket may be disabled if table capacity is too small.
