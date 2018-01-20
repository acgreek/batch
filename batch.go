package main

import (
	"fmt"
	"time"
)

type Batch struct {
	maxItems int
	maxAge int64
	age int64
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
		fmt.Printf("ok is false\n")
		if (currentNumItems != 0) {
			b.completeBatch <- b.incompleteBatch
		}
		b.incompleteBatch = make([]interface {}, 0,  b.maxItems)
		return true
	}
	if (currentNumItems == 0) {
		b.age = time.Now().Unix()
	}
	currentNumItems +=1
	b.incompleteBatch = b.incompleteBatch[:currentNumItems]
	b.incompleteBatch[currentNumItems-1] = item
	if (currentNumItems == b.maxItems) {
		b.completeBatch <- b.incompleteBatch
		b.incompleteBatch = make([]interface {}, 0,  b.maxItems)
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

func batchBuilder(b * Batch) {
	done := false
	for done == false {
		fmt.Printf("sleeping for %d seconds %d\n",maximum(0,b.maxAge - (time.Now().Unix() - b.age)),len(b.incompleteBatch))
		timer := time.NewTimer(time.Duration(maximum(0,b.maxAge - (time.Now().Unix() - b.age))))
		select {
		case item, ok := <- b.items:
			done = appendItemPush(b, ok, item)
		case <- timer.C:
			if (len(b.incompleteBatch) > 0) {
				fmt.Printf("pushed timed out batch\n")
				b.completeBatch <- b.incompleteBatch
				b.incompleteBatch = make([]interface {}, 0,  b.maxItems)
			}
			b.age = time.Now().Unix() + 100000
		}
	}
}

func NewBatch(maxItems, maxAge, consumers int) *Batch{
	b := &Batch{
		maxItems: maxItems,
		maxAge: int64(maxAge),
		age: time.Now().Unix() + 10000,
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
