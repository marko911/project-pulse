// Package nats provides NATS JetStream client infrastructure for distributed event fanout.
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

// Config holds NATS connection configuration.
type Config struct {
	URL            string        // NATS server URL (e.g., "nats://localhost:4222")
	Name           string        // Client connection name for identification
	ReconnectWait  time.Duration // Time to wait between reconnection attempts
	MaxReconnects  int           // Maximum reconnection attempts (-1 for unlimited)
	ConnectTimeout time.Duration // Initial connection timeout
}

// DefaultConfig returns sensible defaults for local development.
func DefaultConfig() Config {
	return Config{
		URL:            "nats://localhost:4222",
		Name:           "pulse-service",
		ReconnectWait:  2 * time.Second,
		MaxReconnects:  -1, // Unlimited
		ConnectTimeout: 10 * time.Second,
	}
}

// Client wraps a NATS connection with JetStream support and lifecycle management.
type Client struct {
	nc  *nats.Conn
	js  jetstream.JetStream
	cfg Config

	mu     sync.RWMutex
	closed bool
}

// Connect establishes a connection to NATS with JetStream enabled.
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

// JetStream returns the JetStream context for stream operations.
func (c *Client) JetStream() jetstream.JetStream {
	return c.js
}

// Conn returns the underlying NATS connection.
func (c *Client) Conn() *nats.Conn {
	return c.nc
}

// IsConnected returns true if the client has an active connection.
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return !c.closed && c.nc.IsConnected()
}

// Close gracefully shuts down the NATS connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	// Drain ensures in-flight messages are processed before closing
	if err := c.nc.Drain(); err != nil {
		c.nc.Close()
		return fmt.Errorf("nats drain: %w", err)
	}

	return nil
}
