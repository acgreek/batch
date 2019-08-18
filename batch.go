// Package batch accumulate elements in to a batch and then push it out
// batch is pushed out if the element limit is reached or timer expires
package batch

import (
	"time"
)

// Batch object that holds the various state for batching
type Batch struct {
	maxItems        int
	maxAge          int64
	age             int64
	incompleteBatch []interface{}
	items           chan interface{}
	completeBatch   chan []interface{}
}

// Append queues an item in the batcher
func (b *Batch) Append(item interface{}) {
	b.items <- item
}

// Scan block until a batch is ready to be processed
func (b *Batch) Scan() []interface{} {
	return <-b.completeBatch
}

// Close clean up the Batch
func (b *Batch) Close() {
	close(b.items)
}
func appendItemPush(b *Batch, ok bool, item interface{}) bool {
	currentNumItems := len(b.incompleteBatch)
	if ok == false {
		if currentNumItems != 0 {
			b.completeBatch <- b.incompleteBatch
		}
		b.incompleteBatch = make([]interface{}, 0, b.maxItems)
		return true
	}
	if currentNumItems == 0 {
		b.age = time.Now().Unix()
	}
	currentNumItems++
	b.incompleteBatch = b.incompleteBatch[:currentNumItems]
	b.incompleteBatch[currentNumItems-1] = item
	if currentNumItems == b.maxItems {
		b.completeBatch <- b.incompleteBatch
		b.incompleteBatch = make([]interface{}, 0, b.maxItems)
		b.age = time.Now().Unix() + 100000
	}
	return false
}
func maximum(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func batchBuilder(b *Batch) {
	done := false
	for done == false {
		timer := time.NewTimer(time.Second * time.Duration(maximum(0, b.maxAge-(time.Now().Unix()-b.age))))
		select {
		case item, ok := <-b.items:
			done = appendItemPush(b, ok, item)
		case <-timer.C:
			if len(b.incompleteBatch) > 0 {
				b.completeBatch <- b.incompleteBatch
				b.incompleteBatch = make([]interface{}, 0, b.maxItems)
			}
			b.age = time.Now().Unix() + 100000
		}
	}
	close(b.completeBatch)
}

// NewBatch returns a new Batcher, no batch will be larger than maxItems, or have elements older than
// maxAge. consumers should be the set to the number of goroutines you expect to have blocking on Scan
func NewBatch(maxItems, maxAge, consumers int) *Batch {
	b := &Batch{
		maxItems:        maxItems,
		maxAge:          int64(maxAge),
		age:             time.Now().Unix() + 10000,
		incompleteBatch: make([]interface{}, 0, maxItems+1),
		items:           make(chan interface{}, maxItems*consumers),
		completeBatch:   make(chan []interface{}, maxItems*consumers),
	}
	go batchBuilder(b)
	return b
}
