// Shared configuration for k6 load tests
// Project Pulse - Milestone 4: Performance & WebSocket Scale

export const config = {
  // Target URLs
  httpUrl: __ENV.HTTP_URL || 'http://localhost:8080',
  wsUrl: __ENV.WS_URL || 'ws://localhost:8080/ws',

  // Performance targets (SLOs)
  targets: {
    p99Latency: 50,       // ms - end-to-end event delivery
    p95Latency: 25,       // ms
    errorRate: 0.001,     // 0.1% max error rate
    throughput: 10000,    // events/sec baseline
  },

  // Test scenarios
  scenarios: {
    // Smoke test: Quick validation
    smoke: {
      vus: 10,
      duration: '1m',
    },
    // Load test: Normal expected load
    load: {
      vus: 100,
      duration: '5m',
    },
    // Stress test: Find breaking point
    stress: {
      stages: [
        { duration: '2m', target: 100 },
        { duration: '5m', target: 500 },
        { duration: '5m', target: 1000 },
        { duration: '5m', target: 2000 },
        { duration: '2m', target: 0 },
      ],
    },
    // Spike test: Sudden traffic surge
    spike: {
      stages: [
        { duration: '1m', target: 100 },
        { duration: '10s', target: 2000 },
        { duration: '3m', target: 2000 },
        { duration: '10s', target: 100 },
        { duration: '1m', target: 0 },
      ],
    },
    // Soak test: Extended duration
    soak: {
      vus: 200,
      duration: '30m',
    },
  },

  // Subscription filter configurations for testing
  filters: {
    simple: {
      chains: ['ethereum'],
      event_types: ['transfer'],
    },
    moderate: {
      chains: ['ethereum', 'solana'],
      event_types: ['transfer', 'swap', 'mint'],
      accounts: ['0x1234567890abcdef1234567890abcdef12345678'],
    },
    complex: {
      chains: ['ethereum', 'solana', 'polygon', 'arbitrum'],
      event_types: ['transfer', 'swap', 'mint', 'burn', 'stake', 'unstake'],
      accounts: [
        '0x1234567890abcdef1234567890abcdef12345678',
        '0xabcdef1234567890abcdef1234567890abcdef12',
        '0x9876543210fedcba9876543210fedcba98765432',
      ],
      program_ids: ['TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'],
    },
  },

  // Chain names for test data generation
  chains: ['ethereum', 'solana', 'polygon', 'arbitrum', 'optimism', 'base'],

  // Event types for test data
  eventTypes: ['transfer', 'swap', 'mint', 'burn', 'stake', 'unstake', 'approve', 'deposit', 'withdraw'],
};

// Generate a random test account address
export function randomAccount() {
  const chars = '0123456789abcdef';
  let addr = '0x';
  for (let i = 0; i < 40; i++) {
    addr += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return addr;
}

// Generate a random transaction hash
export function randomTxHash() {
  const chars = '0123456789abcdef';
  let hash = '0x';
  for (let i = 0; i < 64; i++) {
    hash += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return hash;
}

// Generate a mock canonical event
export function generateEvent(overrides = {}) {
  const chain = config.chains[Math.floor(Math.random() * config.chains.length)];
  const eventType = config.eventTypes[Math.floor(Math.random() * config.eventTypes.length)];

  return {
    event_id: `evt_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
    chain: chain,
    block_num: Math.floor(Math.random() * 10000000) + 18000000,
    tx_hash: randomTxHash(),
    event_type: eventType,
    accounts: [randomAccount(), randomAccount()],
    program_id: '',
    timestamp: new Date().toISOString(),
    native_value: Math.floor(Math.random() * 1000000000000000000),
    ...overrides,
  };
}
