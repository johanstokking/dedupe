package dedupe

import (
	"context"
	"fmt"
	"sync"
	"time"
)

var (
	DeduplicationWindow = 200 * time.Millisecond
	CollectionWindow    = 5 * time.Second
)

type Message struct {
	Val  string
	Time time.Time
}

type Callback func(key string, messages []*Message)

type Interface interface {
	HandleUplink(ctx context.Context, msg *Message)
}

type messagesCh struct {
	mu  sync.RWMutex
	val chan *Message
}

type d struct {
	messages sync.Map

	afterDedupe  func(key string) <-chan struct{}
	afterCollect func(key string) <-chan struct{}
	handler      Callback
	collector    Callback
}

func New(handler, collector Callback) Interface {
	after := func(d time.Duration) func(string) <-chan struct{} {
		return func(_ string) <-chan struct{} {
			ch := make(chan struct{})
			time.AfterFunc(d, func() {
				ch <- struct{}{}
			})
			return ch
		}
	}

	return &d{
		afterDedupe:  after(DeduplicationWindow),
		afterCollect: after(CollectionWindow),
		handler:      handler,
		collector:    collector,
	}
}

func (d *d) HandleUplink(ctx context.Context, msg *Message) {
	key := msg.Val
	nch := &messagesCh{
		val: make(chan *Message),
	}
	actual, loaded := d.messages.LoadOrStore(key, nch)
	ch := actual.(*messagesCh)

	if !loaded {
		go d.dedupe(ctx, key, ch)
	}

	ch.mu.RLock()
	if ch.val != nil {
		ch.val <- msg
	}
	ch.mu.RUnlock()
}

func (d *d) dedupe(ctx context.Context, key string, ch *messagesCh) {
	// Create events when dedupe and collection window end.
	deduped := d.afterDedupe(key)
	collected := d.afterCollect(key)

	// Slice to hold all collected messages.
	var messages []*Message

	for {
		select {
		case <-ctx.Done():
			// Stop when the context is canceled.
			return

		case <-deduped:
			// Handle messages when deduplication window ends.
			go d.handler(key, messages)

		case <-collected:
			// Collect all messages when collection window ends.
			d.messages.Delete(key)
			go d.collector(key, messages)
			// After writing the channel and before reading it here, collection may occur; drain the channel.
			go func() {
				ch.mu.Lock()
				for {
					select {
					case <-ch.val:
						fmt.Printf("%v: draining value\n", key)
					default:
						fmt.Printf("%v: drained channel\n", key)
						close(ch.val)
						ch.val = nil
						ch.mu.Unlock()
						return
					}
				}
			}()
			return

		case msg := <-ch.val:
			// Append incoming messages.
			messages = append(messages, msg)
		}
	}
}
