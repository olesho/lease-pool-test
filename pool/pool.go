package pool

import (
	"context"
	"errors"
	"sync"
)

var ErrPoolClosed = errors.New("ErrPoolClosed")

type Pool[T any] struct {
	lock       sync.Mutex
	free       []T
	closed     bool
	wakeSignal chan struct{}
}

// New creates a pool pre-filled with the given items.
func New[T any](items []T) *Pool[T] {
	return &Pool[T]{
		lock:       sync.Mutex{},
		free:       items,
		closed:     false,
		wakeSignal: make(chan struct{}),
	}
}

// Acquire blocks until an item is free, then leases and returns it.
// If ctx is cancelled or its deadline passes first, it returns the zero
// value and ctx.Err(). If the pool is closed, it returns ErrPoolClosed.
func (p *Pool[T]) Acquire(ctx context.Context) (T, error) {
	var zero T

	if p.closed {
		return zero, ErrPoolClosed
	}

	for {
		if p.closed {
			return zero, ErrPoolClosed
		}

		p.lock.Lock()
		if len(p.free) > 0 {
			item := p.free[0]
			p.free = p.free[1:]
			p.lock.Unlock()
			return item, nil
		}
		p.lock.Unlock()

		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-p.wakeSignal:

		}
	}
}

// Release returns a previously-acquired item to the pool.
func (p *Pool[T]) Release(item T) {
	if p.closed {
		return
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	// the requirements don't say anything about T being "comparable"
	// hence I can't search and release "that exact" item which was acquired,
	// so I use append
	p.free = append(p.free, item)

	// this will release others to acquire
	close(p.wakeSignal)

	// recreate wakeSignal channel
	p.wakeSignal = make(chan struct{})
}

// Close makes the pool reject further Acquire calls and unblocks any waiters.
func (p *Pool[T]) Close() {
	p.closed = true
}
