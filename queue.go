package graylog

import (
	"container/list"
	"sync"
)

type BlockingList struct {
	list *list.List
	ch   chan struct{}
	mu   sync.Mutex
}

func NewBlockingList() *BlockingList {
	return &BlockingList{
		list: list.New(),
		ch:   make(chan struct{}, 1),
	}
}

func (bl *BlockingList) PushBack(v interface{}) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	bl.list.PushBack(v)
	select {
	case bl.ch <- struct{}{}:
	default:
	}
}

func (bl *BlockingList) FrontBlock() interface{} {
	for {
		bl.mu.Lock()
		if e := bl.list.Front(); e != nil {
			bl.list.Remove(e)
			bl.mu.Unlock()
			return e.Value
		}
		bl.mu.Unlock()
		<-bl.ch
	}
}

func (bl *BlockingList) Len() int {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	return bl.list.Len()
}
