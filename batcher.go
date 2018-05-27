package batcher

// Batcher
// API
// Copyright © 2018 Eduard Sesigin. All rights reserved. Contacts: <claygod@yandex.ru>

import (
	//"fmt"
	"runtime"
	"sync"
)

const (
	stateRun int64 = iota
	stateStop
)

type Batcher struct {
	batchSize int
	barrier   int64
	wal       Wal
	queue     Queue
}

func NewBatcher(size int, wal Wal) *Batcher {
	return &Batcher{
		batchSize: size,
		barrier:   stateRun,
		wal:       wal,
	}
}

func (b *Batcher) worker(level int) {
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

func (b *Batcher) inputProcess(in *func(), wg *sync.WaitGroup) {
	(*in)()
	wg.Done()
}

/*
type Handler interface {
	Do(*Input)
}
*/
type Queue interface {
	GetBatch(int) []*func() //Input
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
