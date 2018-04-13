package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/johanstokking/dedupe/dedupe"
)

func deduped(key string, messages []*dedupe.Message) {
	fmt.Printf("%v: deduped %d\n", key, len(messages))
}

func collected(key string, messages []*dedupe.Message) {
	fmt.Printf("%v: collected %d\n", key, len(messages))
}

func main() {
	dedupe.DeduplicationWindow = 2 * time.Second
	dedupe.CollectionWindow = 5 * time.Second

	ctx := context.Background()

	fmt.Println("type text and press enter")

	d := dedupe.New(deduped, collected)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		d.HandleUplink(ctx, &dedupe.Message{
			Time: time.Now(),
			Val:  scanner.Text(),
		})
	}
	if scanner.Err() != nil {
		panic(scanner.Err())
	}
}
