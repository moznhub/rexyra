package goredis

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// receive waits for the next message on ch.
func receive(t *testing.T, ch <-chan *redis.Message) *redis.Message {
	t.Helper()
	select {
	case m, ok := <-ch:
		if !ok {
			t.Fatal("the message channel was closed")
		}
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("no message within 5 seconds")
	}
	return nil
}

func TestPubSub(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		sub := c.Subscribe(ctx(), p+"news")
		defer sub.Close()
		// Wait for the subscription to be confirmed before publishing.
		s := must(sub.Receive(ctx()))
		equal(t, "confirmation", fmt.Sprint(s), fmt.Sprintf("subscribe: %snews", p))
		ch := sub.Channel()

		equal(t, "PUBLISH receivers", must(c.Publish(ctx(), p+"news", "hello").Result()), 1)
		m := receive(t, ch)
		equal(t, "message", m.Channel+" "+m.Payload, p+"news hello")
		equal(t, "PUBLISH to nobody", must(c.Publish(ctx(), p+"nobody", "x").Result()), 0)

		// Order is kept, and binary payloads survive.
		for i := 0; i < 500; i++ {
			must(c.Publish(ctx(), p+"news", fmt.Sprintf("m%d", i)).Result())
		}
		for i := 0; i < 500; i++ {
			equal(t, "order", receive(t, ch).Payload, fmt.Sprintf("m%d", i))
		}
		must(c.Publish(ctx(), p+"news", "\x00\r\n\xff").Result())
		equal(t, "binary payload", receive(t, ch).Payload, "\x00\r\n\xff")

		// Patterns.
		must(0, sub.PSubscribe(ctx(), p+"events.*"))
		waitFor(t, func() bool { return must(c.PubSubNumPat(ctx()).Result()) >= 1 })
		equal(t, "PUBLISH to a pattern", must(c.Publish(ctx(), p+"events.login", "ada").Result()), 1)
		m = receive(t, ch)
		equal(t, "pattern message", m.Pattern+" "+m.Channel+" "+m.Payload, p+"events.* "+p+"events.login ada")

		// More channels on the same subscriber, and the introspection commands.
		must(0, sub.Subscribe(ctx(), p+"a", p+"b"))
		waitFor(t, func() bool { return len(must(c.PubSubChannels(ctx(), p+"*").Result())) == 3 })
		chans := must(c.PubSubChannels(ctx(), p+"*").Result())
		sort.Strings(chans)
		equal(t, "PUBSUB CHANNELS", strings.Join(chans, ","), p+"a,"+p+"b,"+p+"news")
		numsub := must(c.PubSubNumSub(ctx(), p+"a", p+"none").Result())
		equal(t, "PUBSUB NUMSUB", fmt.Sprint(numsub[p+"a"], numsub[p+"none"]), "1 0")

		// PING while subscribed (go-redis's health check does this).
		must(0, sub.Ping(ctx(), "hi"))

		must(0, sub.Unsubscribe(ctx(), p+"a"))
		waitFor(t, func() bool { return must(c.PubSubNumSub(ctx(), p+"a").Result())[p+"a"] == 0 })
		must(c.Publish(ctx(), p+"a", "dropped").Result())
		must(c.Publish(ctx(), p+"b", "kept").Result())
		equal(t, "after UNSUBSCRIBE", receive(t, ch).Payload, "kept")

		must(0, sub.PUnsubscribe(ctx()))
		waitFor(t, func() bool { return must(c.Publish(ctx(), p+"events.x", "x").Result()) == 0 })
	})
}

// Sharded Pub/Sub (Redis 7): SSUBSCRIBE / SPUBLISH.
func TestShardedPubSub(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		sub := c.SSubscribe(ctx(), p+"shard")
		defer sub.Close()
		must(sub.Receive(ctx()))
		ch := sub.Channel()
		equal(t, "SPUBLISH receivers", must(c.SPublish(ctx(), p+"shard", "hi").Result()), 1)
		m := receive(t, ch)
		equal(t, "sharded message", m.Channel+" "+m.Payload, p+"shard hi")
		equal(t, "PUBSUB SHARDCHANNELS", strings.Join(must(c.PubSubShardChannels(ctx(), p+"*").Result()), ","), p+"shard")
		equal(t, "PUBSUB SHARDNUMSUB", must(c.PubSubShardNumSub(ctx(), p+"shard").Result())[p+"shard"], 1)
	})
}

// go-redis resubscribes by itself after the connection of a PubSub is dropped.
func TestPubSubResubscribesAfterClientKill(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		opt := options(c.Options().Protocol)
		opt.ClientName = "go-pubsub-" + p[3:11]
		subscriber := connect(t, opt)
		sub := subscriber.Subscribe(ctx(), p+"chan")
		defer sub.Close()
		must(sub.Receive(ctx()))
		ch := sub.Channel()

		killed := must(c.ClientKillByFilter(ctx(), "TYPE", "pubsub", "SKIPME", "yes").Result())
		if killed < 1 {
			t.Fatalf("CLIENT KILL TYPE pubsub killed %d connections", killed)
		}

		// Publish until the resubscribed connection receives a message.
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			must(c.Publish(ctx(), p+"chan", "after").Result())
			select {
			case m := <-ch:
				equal(t, "message after reconnect", m.Payload, "after")
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
		t.Fatal("the subscriber did not receive a message after its connection was killed")
	})
}

// waitFor polls cond until it is true or 5 seconds pass.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 5 seconds")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
