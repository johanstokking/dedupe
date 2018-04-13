package dedupe

import (
	"context"
	"testing"
	"time"

	"github.com/smartystreets/assertions"
	"github.com/smartystreets/assertions/should"
)

func TestDedupe(t *testing.T) {
	dedupeChs := make(map[string]chan struct{})
	collectChs := make(map[string]chan struct{})
	handled := make(map[string]chan int)
	collected := make(map[string]chan int)

	d := &d{
		// Instead of time functions, return a channel we control.
		afterDedupe: func(key string) <-chan struct{} {
			ch := make(chan struct{})
			dedupeChs[key] = ch
			return ch
		},
		afterCollect: func(key string) <-chan struct{} {
			ch := make(chan struct{})
			collectChs[key] = ch
			return ch
		},
		// Instead of callbacks, write the value to a channel.
		handler: func(key string, messages []*Message) {
			handled[key] <- len(messages)
		},
		collector: func(key string, messages []*Message) {
			collected[key] <- len(messages)
		},
	}

	ctx := context.Background()

	t.Run("Normal", func(t *testing.T) {
		a := assertions.New(t)

		handled["a"] = make(chan int)
		collected["a"] = make(chan int)

		d.HandleUplink(ctx, &Message{"a", time.Now()})
		d.HandleUplink(ctx, &Message{"a", time.Now()})
		d.HandleUplink(ctx, &Message{"a", time.Now()})

		// Deduplication window.
		dedupeChs["a"] <- struct{}{}
		a.So(<-handled["a"], should.Equal, 3)
		close(dedupeChs["a"])

		d.HandleUplink(ctx, &Message{"a", time.Now()})

		// Collection window.
		collectChs["a"] <- struct{}{}
		a.So(<-collected["a"], should.Equal, 4)
		close(collectChs["a"])
	})

	// TODO: More test cases.
}
