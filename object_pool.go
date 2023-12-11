package graylog

import (
	"fmt"
	"sync"
	"time"
)

type ObjectPool struct {
	mu           sync.Mutex
	list         *BlockingList
	factory      func() (interface{}, error)
	capacity     int
	createdCount int
}

func NewObjectPool(factory func() (interface{}, error), capacity int) *ObjectPool {
	if capacity <= 0 {
		capacity = 1
	}

	return &ObjectPool{
		list:     NewBlockingList(),
		factory:  factory,
		capacity: capacity,
	}
}

func (p *ObjectPool) Get() interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.createdCount < p.capacity {
		for {
			obj, err := p.factory()
			if err != nil {
				fmt.Printf("create obj failed: %s\n", err)
				time.Sleep(1 * time.Second)
				continue
			}
			p.createdCount += 1
			return obj
		}
	}
	return p.list.FrontBlock()
}

func (p *ObjectPool) Put(obj interface{}) {
	p.list.PushBack(obj)
}
