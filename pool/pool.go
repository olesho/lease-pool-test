package pool

import "context"

type Pool[T any] struct {
	items []T
}

// New creates a pool pre-filled with the given items.
func New[T any](items []T) *Pool[T] {
	return &Pool[T]{
		items: items,
	}
}

// Acquire blocks until an item is free, then leases and returns it.
// If ctx is cancelled or its deadline passes first, it returns the zero
// value and ctx.Err(). If the pool is closed, it returns ErrPoolClosed.
func (p *Pool[T]) Acquire(ctx context.Context) (T, error) {
	var t T
	return t, nil
}

// Release returns a previously-acquired item to the pool.
func (p *Pool[T]) Release(item T) {

}

// Close makes the pool reject further Acquire calls and unblocks any waiters.
func (p *Pool[T]) Close() {

}
