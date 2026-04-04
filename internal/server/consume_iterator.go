package server

import (
	"context"

	logstore "github.com/maksim/camu/internal/log"
)

type consumeIterItem struct {
	msg  logstore.Message
	err  error
	done bool
}

type consumeIterator struct {
	ch   <-chan consumeIterItem
	next *logstore.Message
	err  error
	done bool
}

func startConsumeIterator(ctx context.Context, walk func(func(logstore.Message) bool) error) *consumeIterator {
	ch := make(chan consumeIterItem, 1)
	go func() {
		defer close(ch)
		err := walk(func(msg logstore.Message) bool {
			select {
			case <-ctx.Done():
				return false
			case ch <- consumeIterItem{msg: msg}:
				return true
			}
		})
		if err != nil {
			select {
			case <-ctx.Done():
			case ch <- consumeIterItem{err: err, done: true}:
			}
			return
		}
		select {
		case <-ctx.Done():
		case ch <- consumeIterItem{done: true}:
		}
	}()
	return &consumeIterator{ch: ch}
}

func (it *consumeIterator) peek() (*logstore.Message, error) {
	if it.done {
		return nil, it.err
	}
	if it.next != nil {
		return it.next, nil
	}
	item, ok := <-it.ch
	if !ok {
		it.done = true
		return nil, it.err
	}
	if item.err != nil {
		it.err = item.err
		it.done = true
		return nil, item.err
	}
	if item.done {
		it.done = true
		return nil, nil
	}
	msg := item.msg
	it.next = &msg
	return it.next, nil
}

func (it *consumeIterator) pop() {
	it.next = nil
}
