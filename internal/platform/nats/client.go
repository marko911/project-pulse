package nats

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Config struct {
	URL            string
	Name           string
	ReconnectWait  time.Duration
	MaxReconnects  int
	ConnectTimeout time.Duration
}

func DefaultConfig() Config {
	return Config{
		URL:            "nats://localhost:4222",
		Name:           "pulse-service",
		ReconnectWait:  2 * time.Second,
		MaxReconnects:  -1,
		ConnectTimeout: 10 * time.Second,
	}
}

type Client struct {
	nc  *nats.Conn
	js  jetstream.JetStream
	cfg Config

	mu     sync.RWMutex
	closed bool
}

func Connect(ctx context.Context, cfg Config) (*Client, error) {
	opts := []nats.Option{
		nats.Name(cfg.Name),
		nats.ReconnectWait(cfg.ReconnectWait),
		nats.MaxReconnects(cfg.MaxReconnects),
		nats.Timeout(cfg.ConnectTimeout),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				log.Printf("nats: disconnected: %v", err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("nats: reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Printf("nats: connection closed")
		}),
	}

	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream init: %w", err)
	}

	return &Client{
		nc:  nc,
		js:  js,
		cfg: cfg,
	}, nil
}

func (c *Client) JetStream() jetstream.JetStream {
	return c.js
}

func (c *Client) Conn() *nats.Conn {
	return c.nc
}

func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return !c.closed && c.nc.IsConnected()
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	if err := c.nc.Drain(); err != nil {
		c.nc.Close()
		return fmt.Errorf("nats drain: %w", err)
	}

	return nil
}
