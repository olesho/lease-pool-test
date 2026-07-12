package pool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPool_AcquireSingle(t *testing.T) {
	p := New([]string{"hello"})
	item, _ := p.Acquire(context.Background())
	if item != "hello" {
		t.Errorf("got %q, want hello", item)
	}
}

func TestPool_ReleaseSingle(t *testing.T) {
	hello := "hello"
	p := New([]string{hello})
	item1, _ := p.Acquire(context.Background())
	if item1 != "hello" {
		t.Errorf("got %q, want hello", item1)
	}
	// exhausted here: 0 to acquire

	sequence := make(chan string, 2)

	go func() {
		item2, _ := p.Acquire(context.Background())
		if item2 != hello {
			t.Errorf("got %q, want hello", item2)
		}
		sequence <- item2
	}()

	go func() {
		p.Release(item1)
		sequence <- "RELEASE"
	}()

	s1 := <-sequence
	s2 := <-sequence

	if s1 == hello {
		t.Error("acquired before release")
	}
	if s2 == "RELEASE" {
		t.Error("release did happen after acquire")
	}
	if s2 != hello {
		t.Error("acquire didn't happen after release")
	}
	if s1 != "RELEASE" {
		t.Error("release should happen first")
	}
}

func TestCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	hello := "hello"
	p := New([]string{hello})
	item1, _ := p.Acquire(ctx)
	if item1 != "hello" {
		t.Errorf("got %q, want hello", item1)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		item, err := p.Acquire(ctx)
		if item != "" {
			t.Errorf("got %q, want hello", item1)
		}

		if !errors.Is(err, context.Canceled) {
			t.Error("ctx cancellation didn't happen")
		}
		wg.Done()
	}()

	cancel()
	wg.Wait()
}

func TestCtxDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(1*time.Millisecond))

	hello := "hello"
	p := New([]string{hello})
	item1, _ := p.Acquire(ctx)
	if item1 != "hello" {
		t.Errorf("got %q, want hello", item1)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		item, err := p.Acquire(ctx)
		if item != "" {
			t.Errorf("got %q, want hello", item1)
		}

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Error("ctx deadline didn't happen")
		}
		wg.Done()
	}()

	wg.Wait()
	cancel()
}

func TestPool_Close(t *testing.T) {
	hello := "hello"
	p := New([]string{hello})
	item1, _ := p.Acquire(context.Background())
	if item1 != "hello" {
		t.Errorf("got %q, want hello", item1)
	}
	// exhausted here: 0 to acquire

	result := make(chan string, 1)
	go func() {
		item, err := p.Acquire(context.Background())
		if !errors.Is(err, ErrPoolClosed) {
			t.Errorf("got %v, want ErrPoolClosed", err)
		}
		result <- item
	}()

	p.Close()

	// should unblock
	s := <-result
	if s != "" {
		t.Errorf("got %q, want '' due to p.Close()", s)
	}

	// should be rejected
	p.Release(item1)
}
