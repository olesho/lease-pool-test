package pool

import (
	"context"
	"errors"
	"sync"
)

var ErrPoolClosed = errors.New("ErrPoolClosed")

type Pool[T any] struct {
	lock   sync.Mutex
	free   []T
	closed bool
}

// New creates a pool pre-filled with the given items.
func New[T any](items []T) *Pool[T] {
	return &Pool[T]{
		lock: sync.Mutex{},
		free: items,
	}
}

// Acquire blocks until an item is free, then leases and returns it.
// If ctx is cancelled or its deadline passes first, it returns the zero
// value and ctx.Err(). If the pool is closed, it returns ErrPoolClosed.
func (p *Pool[T]) Acquire(ctx context.Context) (T, error) {
	var t T

	if p.closed {
		return t, ErrPoolClosed
	}

	for {
		select {
		case <-ctx.Done():
			return t, ctx.Err()
		default:
			if item, ok := p.tryEvict(); ok {
				return item, nil
			}
		}
	}
}

// Release returns a previously-acquired item to the pool.
func (p *Pool[T]) Release(item T) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.closed {
		return
	}

	p.free = append(p.free, item)
}

// Close makes the pool reject further Acquire calls and unblocks any waiters.
func (p *Pool[T]) Close() {
	p.closed = true
}

func (p *Pool[T]) tryEvict() (T, bool) {
	// this should work but might be inefficient
	p.lock.Lock()
	defer p.lock.Unlock()

	var t T
	if len(p.free) == 0 {
		return t, false
	}

	item := p.free[0]
	p.free = p.free[1:]
	return item, true
}
