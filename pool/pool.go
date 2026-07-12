package pool

import (
	"context"
	"errors"
	"sync/atomic"
)

var ErrPoolClosed = errors.New("ErrPoolClosed")

type Pool[T any] struct {
	free        chan T
	closed      atomic.Bool
	closeSignal chan struct{}
	inUse       atomic.Int64
}

// New creates a pool pre-filled with the given items.
func New[T any](items []T) *Pool[T] {
	p := &Pool[T]{
		free:        make(chan T, len(items)),
		closed:      atomic.Bool{},
		closeSignal: make(chan struct{}, 1),
	}
	p.closed.Store(false)
	for _, item := range items {
		p.free <- item
	}
	return p
}

// Acquire blocks until an item is free, then leases and returns it.
// If ctx is cancelled or its deadline passes first, it returns the zero
// value and ctx.Err(). If the pool is closed, it returns ErrPoolClosed.
func (p *Pool[T]) Acquire(ctx context.Context) (T, error) {
	var zero T

	if p.closed.Load() {
		return zero, ErrPoolClosed
	}

	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case item := <-p.free:
		if p.closed.Load() {
			return zero, ErrPoolClosed
		}
		p.inUse.Add(1)
		return item, nil
	case <-p.closeSignal:
		return zero, ErrPoolClosed
	}
}

// Release returns a previously-acquired item to the pool.
func (p *Pool[T]) Release(item T) {

	if p.closed.Load() {
		return
	}

	for {
		n := p.inUse.Load()
		if n == 0 {
			// panic("pool: release without a matching acquire")
			// this is not clearly documented in the requirements
			// what should happen if Release attempted without preceding Acquire?
			return
		}

		if p.inUse.CompareAndSwap(n, n-1) {
			break
		}
	}

	select {

	// the requirements don't say anything about T being "comparable"
	// hence I can't search and release "that exact" item which was acquired,
	// so I assume caller will always stick to the contract and only send item T
	// where item belongs to initial set of items
	case p.free <- item:

	case <-p.closeSignal:
	}
}

// Close makes the pool reject further Acquire calls and unblocks any waiters.
func (p *Pool[T]) Close() {
	if p.closed.Load() {
		return
	}
	p.closed.Store(true)
	close(p.closeSignal)
}
