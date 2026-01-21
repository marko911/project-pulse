export const config = {
  httpUrl: __ENV.HTTP_URL || 'http://localhost:8080',
  wsUrl: __ENV.WS_URL || 'ws://localhost:8080/ws',

  targets: {
    p99Latency: 50,
    p95Latency: 25,
    errorRate: 0.001,
    throughput: 10000,
  },

  scenarios: {
    smoke: {
      vus: 10,
      duration: '1m',
    },
    load: {
      vus: 100,
      duration: '5m',
    },
    stress: {
      stages: [
        { duration: '2m', target: 100 },
        { duration: '5m', target: 500 },
        { duration: '5m', target: 1000 },
        { duration: '5m', target: 2000 },
        { duration: '2m', target: 0 },
      ],
    },
    spike: {
      stages: [
        { duration: '1m', target: 100 },
        { duration: '10s', target: 2000 },
        { duration: '3m', target: 2000 },
        { duration: '10s', target: 100 },
        { duration: '1m', target: 0 },
      ],
    },
    soak: {
      vus: 200,
      duration: '30m',
    },
  },

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

  chains: ['ethereum', 'solana', 'polygon', 'arbitrum', 'optimism', 'base'],

  eventTypes: ['transfer', 'swap', 'mint', 'burn', 'stake', 'unstake', 'approve', 'deposit', 'withdraw'],
};

export function randomAccount() {
  const chars = '0123456789abcdef';
  let addr = '0x';
  for (let i = 0; i < 40; i++) {
    addr += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return addr;
}

export function randomTxHash() {
  const chars = '0123456789abcdef';
  let hash = '0x';
  for (let i = 0; i < 64; i++) {
    hash += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return hash;
}

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
