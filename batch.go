package main

import (
	"fmt"
	"time"
)

type Batch struct {
	maxItems int
	maxAge int
	age time.Time
	incompleteBatch []interface {}
	items  chan interface {}
	completeBatch chan []interface {}
}

func (b * Batch) Append(item interface{}) {
	b.items <- item
}

func (b * Batch) Scan() []interface{}  {
	return <-b.completeBatch
}

func (b * Batch) Close() {
	close(b.items)
}
func appendItemPush(b * Batch, ok bool, item interface {}) bool {
	currentNumItems := len(b.incompleteBatch)
	if (ok ==false) {
		if (currentNumItems != 0) {
			b.completeBatch <- b.incompleteBatch
		}
		b.incompleteBatch = make([]interface {}, 0,  b.maxItems)
		return true
	}
	if (currentNumItems == 0) {
		b.age = time.Now()
	}
	currentNumItems +=1
	b.incompleteBatch = b.incompleteBatch[:currentNumItems]
	b.incompleteBatch[currentNumItems-1] = item
	if (currentNumItems == b.maxItems) {
		b.completeBatch <- b.incompleteBatch
		b.incompleteBatch = make([]interface {}, 0,  b.maxItems)
		b.age = time.Now()
	}
	return false
}

func batchBuilder(b * Batch) {
	done := false
	for done == false {
		select {
		case item, ok := <- b.items:
			done = appendItemPush(b, ok, item)
		case <- time.After(b.maxAge * time.Second):
			if (len(b.completBatch) >0) {
				b.completeBatch <- b.incompleteBatch
				b.incompleteBatch = make([]interface {}, 0,  b.maxItems)
				b.age = 0
			}
		}
	}
}

func NewBatch(maxItems, maxAge, consumers int) *Batch{
	b := &Batch{
		maxItems: maxItems,
		maxAge: maxAge,
		incompleteBatch: make([]interface {}, 0, maxItems+1),
		items: make(chan interface {}, maxItems * consumers),
		completeBatch: make(chan []interface {}, maxItems * consumers),
	}
	go batchBuilder(b)
	return b;
}

func main() {
	fmt.Println("vim-go")
}
