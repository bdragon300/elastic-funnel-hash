# Elastic/Funnel hashing PoC

This is a PoC of **funnel hashing** and **elastic hashing** algorithms in Go.

Based on the Paper [Optimal Bounds for Open Addressing Without Reordering](https://arxiv.org/abs/2501.02305) by Martin Farach-Colton, Andrew Krapivin, William Kuszmaul.

This package contains `funnel` and `elastic` packages with the particular implementations. Every `HashTable` type has
`Insert`, `Get`, `Set`, `Len` and `Cap` methods.

Because these hash tables are the PoC:

* They don't support key deletion, race detection, etc.
* Key-value has `[]byte` type
* Tables have the fixed capacity as described in the Paper
* Because of the previous point, they are not resized (this could be achieved by using overflow buckets or data
  "evacuation" mechanism, as it done in Go's `map` type, or similar techniques)
* Because of the previous point, hash tables may accept fewer insertions than their capacity due to a particular data 
  bank overflow, especially if key hashes are not distributed evenly. Keep this in mind when setting the table capacity.
* All internal variables are publicity accessible

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

## Run tests

```shell
go test -v ./...
```

# Elastic hashing

Basically, this is the variant of hash table with open addressing, where the addresses are divided in data banks of geometrically
decreasing sizes of power of 2. Every bank keeps its metric that counts the inserts made into this bank. The lookups and inserts relies on these metrics.

All data slots are divided into banks (sub-arrays `A1..Alog2(n)`) with fixed size geometrically decreasing by the power of 2.

Before the insertion or lookup we select a consecutive banks pair (sub-arrays `Ai` and `Ai+1`) to work on based on key hash.
The exception is the first table's bank `A1`, which is used without a pair.

For insertions, we decide based on metrics of each bank in the pair which of them should be used. After that,
we insert the key-value into the selected bank. Once banks in the pair become full, the subsequent insertions into
them will fail.

For lookups, we do limited probes in the 1st bank in pair, then lookup in 2nd bank and then go back and probe the
remaining slots in the 1st bank.

To resolve collisions, we use the uniform random probing.


# Funnel hashing

This is the variant of hash table with open addressing, where the data is divided in data banks of geometrically
decreasing sizes. Unlike the elastic hashing, every bank is additionally divided by some number of buckets with equal size.
Also, this algorithm uses two additional overflow mini hash tables with different implementations.

Basically, all data is divided into three unequal parts:

1. 90-95% of data is stored in fixed count of banks, each consists of fixed size buckets. The number of buckets
   in every bank geometrically decreases from table start towards the end. Bucket size, bucket
   count and banks count are calculated upon table creation.
2. Overflow1 bucket, that actually is a separate mini-hashtable supporting the uniform random probing.
   May occupy up to 5% of the table.
3. Overflow2 bucket, that is a separate mini-hashtable supporting the two-choice hashing containing the fixed size buckets.
   May occupy up to 5% of the table.

Inserts and lookups always start from the 1st bank. For every bank, we select a bucket based on key hash. 
To resolve collisions, we use the linear probing. If no luck (bucket is full on insert or doesn't contain a key 
we're looking for on lookup), we hop to the next bank.
Once the last bank is reached, we start to process the overflow1 bucket. If it fails, we continue with the
overflow2 bucket. If the overflow2 bucket fails, the process is failed.

Overflow2 bucket may be disabled if table capacity is too small.
