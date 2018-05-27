package batcher

// Batcher
// API
// Copyright © 2018 Eduard Sesigin. All rights reserved. Contacts: <claygod@yandex.ru>

import (
	//"fmt"
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
}

func NewBatcher(wal Wal) *Batcher {
	return &Batcher{
		batchSize: batchSize,
		barrier:   stateRun,
		wal:       wal,
	}
}

func (b *Batcher) Start() {
	if atomic.CompareAndSwapInt64(&b.barrier, stateStop, stateRun) {
		b.worker()
	}
}

func (b *Batcher) Stop() {
	atomic.StoreInt64(&b.barrier, stateStop)
}

func (b *Batcher) SetBatcSize(size int64) {
	atomic.StoreInt64(&b.batchSize, stateStop)
}

func (b *Batcher) worker() {
	var wg sync.WaitGroup
	for {
		batch := b.queue.GetBatch(b.batchSize)

		if len(batch) == 0 {
			runtime.Gosched()
			// time.Sleep(30 * time.Microsecond)
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

func (b *Batcher) inputProcess(in *func() (int64, []byte), wg *sync.WaitGroup) {
	b.wal.Log((*in)())
	wg.Done()
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
