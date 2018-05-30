package batcher

// Batcher
// API
// Copyright © 2018 Eduard Sesigin. All rights reserved. Contacts: <claygod@yandex.ru>

import (
	// "fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

const batchSize int64 = iota

const (
	stateRun int64 = iota
	stateStop
)

type Batcher struct {
	batchSize int64
	barrier   int64
	wal       Wal
	queue     Queue
	wg        sync.WaitGroup
}

func New(wal Wal, queue Queue) *Batcher {
	return &Batcher{
		batchSize: batchSize,
		barrier:   stateRun,
		wal:       wal,
		queue:     queue,
		wg:        sync.WaitGroup{},
	}
}

func (b *Batcher) Start() *Batcher {
	if atomic.CompareAndSwapInt64(&b.barrier, stateStop, stateRun) {
		go b.workerFull()
	}
	return b
}

func (b *Batcher) startCut() *Batcher {
	if atomic.CompareAndSwapInt64(&b.barrier, stateStop, stateRun) {
		go b.workerCut()
	}
	return b
}

func (b *Batcher) Stop() *Batcher {
	atomic.StoreInt64(&b.barrier, stateStop)
	return b
}

func (b *Batcher) SetBatchSize(size int64) *Batcher {
	atomic.StoreInt64(&b.batchSize, stateStop)
	return b
}

func (b *Batcher) workerFull() {
	var wg sync.WaitGroup
	for {
		batch := b.queue.GetBatch(b.batchSize)

		if len(batch) == 0 {
			runtime.Gosched()
			continue
		}
		for _, in := range batch {
			wg.Add(1)
			go b.inputProcess(in, &wg)
		}
		wg.Wait()

		if !b.wal.Save() || b.barrier == stateStop {
			return
		}
	}
}

func (b *Batcher) workerCut() {
	for {
		b.work()
		if !b.wal.Save() || b.barrier == stateStop {
			return
		}
	}
}

func (b *Batcher) work() {
	//var wg sync.WaitGroup = b.wg
	batch := b.queue.GetBatch(b.batchSize)
	if len(batch) == 0 {
		runtime.Gosched()
		return
	}
	for _, in := range batch {
		//wg.Add(1)
		b.inputProcess(in, nil) // &wg
	}
	//wg.Wait()
}

func (b *Batcher) inputProcess(in *func() (int64, []byte), wg *sync.WaitGroup) {
	b.wal.Log((*in)())
	//wg.Done()
}

/*
type Handler interface {
	Do(*Input)
}
*/
type Queue interface {
	GetBatch(int64) []*func() (int64, []byte) //Input
}

type Wal interface {
	Log(int64, []byte) // key, log
	Save() bool
}

/*
type Output struct {
	code    int64
	context interface{}
}
*/
/*
type Input struct {
	number  int64
	handler Handler
	log     []byte
	context interface{}
}
*/
