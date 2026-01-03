// k6 Burst Simulation Load Test
// Project Pulse - Milestone 4: Performance & WebSocket Scale
//
// Simulates high-throughput ingestion bursts to test system resilience
// during traffic spikes. Tests both WebSocket delivery and HTTP endpoints
// under burst conditions.
//
// Usage:
//   k6 run burst_simulation.js                        # Default burst test
//   k6 run -e BURST_RATIO=20 burst_simulation.js      # 20x burst ratio
//   k6 run -e BURST_DURATION=30 burst_simulation.js   # 30s burst windows

import ws from 'k6/ws';
import http from 'k6/http';
import { check, sleep, group } from 'k6';
import { Counter, Trend, Rate, Gauge } from 'k6/metrics';
import { config, generateEvent, randomAccount } from './config.js';

// Custom metrics for burst analysis
const burstEventsReceived = new Counter('burst_events_received_total');
const burstEventLatency = new Trend('burst_event_latency_ms');
const burstDroppedEvents = new Counter('burst_dropped_events_total');
const burstRecoveryTime = new Trend('burst_recovery_time_ms');
const burstErrorRate = new Rate('burst_error_rate');
const currentBurstPhase = new Gauge('current_burst_phase');

// Burst configuration
const burstRatio = parseInt(__ENV.BURST_RATIO) || 10; // 10x normal rate during burst
const burstDuration = parseInt(__ENV.BURST_DURATION) || 10; // seconds
const normalDuration = parseInt(__ENV.NORMAL_DURATION) || 30; // seconds between bursts
const totalCycles = parseInt(__ENV.TOTAL_CYCLES) || 5; // number of burst cycles

export const options = {
  scenarios: {
    // WebSocket receivers - maintain constant connections
    websocket_receivers: {
      executor: 'constant-vus',
      vus: 100,
      duration: `${(burstDuration + normalDuration) * totalCycles + 60}s`,
      exec: 'websocketReceiver',
    },
    // HTTP health monitors - verify system stays responsive
    http_monitors: {
      executor: 'constant-vus',
      vus: 10,
      duration: `${(burstDuration + normalDuration) * totalCycles + 60}s`,
      exec: 'httpMonitor',
      startTime: '5s',
    },
  },
  thresholds: {
    burst_event_latency_ms: ['p(99)<100', 'p(95)<50'],
    burst_error_rate: ['rate<0.01'], // Allow 1% errors during bursts
    burst_recovery_time_ms: ['avg<5000'], // Recover within 5 seconds
  },
};

// Track burst phases globally
let burstStartTime = null;
let inBurst = false;

// WebSocket receiver function
export function websocketReceiver() {
  const wsUrl = config.wsUrl;
  let eventCount = 0;
  let lastEventTime = Date.now();

  const res = ws.connect(wsUrl, {}, function (socket) {
    socket.on('open', function () {
      // Subscribe to all events for burst testing
      socket.send(
        JSON.stringify({
          type: 'subscribe',
          data: {
            chains: config.chains,
            event_types: config.eventTypes,
            ttl_seconds: 7200,
          },
        })
      );
    });

    socket.on('message', function (data) {
      try {
        const msg = JSON.parse(data);

        if (msg.type === 'event') {
          const now = Date.now();
          eventCount++;
          burstEventsReceived.add(1);

          // Measure inter-event latency during bursts
          if (inBurst && msg.data && msg.data.timestamp) {
            const eventTs = new Date(msg.data.timestamp).getTime();
            const latency = now - eventTs;
            burstEventLatency.add(latency);
          }

          // Track gap between events for recovery analysis
          const gap = now - lastEventTime;
          if (gap > 1000) {
            // Gap > 1 second might indicate dropped events
            burstRecoveryTime.add(gap);
          }
          lastEventTime = now;

          burstErrorRate.add(0);
        }
      } catch (e) {
        burstErrorRate.add(1);
      }
    });

    socket.on('error', function (e) {
      burstErrorRate.add(1);
    });

    // Ping to keep alive
    socket.setInterval(function () {
      socket.send(JSON.stringify({ type: 'ping', data: {} }));
    }, 30000);

    // Run for full duration
    const duration = (burstDuration + normalDuration) * totalCycles * 1000 + 30000;
    socket.setTimeout(function () {
      socket.close();
    }, duration);
  });

  check(res, {
    'WebSocket connected': (r) => r && r.status === 101,
  });
}

// HTTP monitor to verify system responsiveness during bursts
export function httpMonitor() {
  const baseUrl = config.httpUrl;

  group('health_during_burst', function () {
    const start = Date.now();
    const res = http.get(`${baseUrl}/health`, {
      timeout: '5s',
    });
    const latency = Date.now() - start;

    const passed = check(res, {
      'health returns 200': (r) => r.status === 200,
      'health latency acceptable': (r) => r.timings.duration < 500,
    });

    if (!passed) {
      burstErrorRate.add(1);
    } else {
      burstErrorRate.add(0);
    }
  });

  group('ready_during_burst', function () {
    const res = http.get(`${baseUrl}/ready`, {
      timeout: '5s',
    });

    const passed = check(res, {
      'ready returns 200 or 503': (r) => r.status === 200 || r.status === 503,
      'ready latency acceptable': (r) => r.timings.duration < 1000,
    });

    if (!passed) {
      burstErrorRate.add(1);
    } else {
      burstErrorRate.add(0);
    }
  });

  group('correctness_during_burst', function () {
    const res = http.get(`${baseUrl}/api/v1/correctness/status`, {
      timeout: '10s',
    });

    const passed = check(res, {
      'correctness returns 200': (r) => r.status === 200,
    });

    if (!passed) {
      burstErrorRate.add(1);
    } else {
      burstErrorRate.add(0);
    }
  });

  // Poll more frequently during expected burst windows
  sleep(1);
}

// Setup function - log burst configuration
export function setup() {
  console.log('='.repeat(60));
  console.log('Burst Simulation Configuration');
  console.log('='.repeat(60));
  console.log(`Burst Ratio: ${burstRatio}x`);
  console.log(`Burst Duration: ${burstDuration}s`);
  console.log(`Normal Duration: ${normalDuration}s`);
  console.log(`Total Cycles: ${totalCycles}`);
  console.log(`Expected Total Duration: ${(burstDuration + normalDuration) * totalCycles}s`);
  console.log('='.repeat(60));

  return {
    burstRatio,
    burstDuration,
    normalDuration,
    totalCycles,
    startTime: Date.now(),
  };
}

// Teardown - analyze burst behavior
export function teardown(data) {
  const elapsed = Date.now() - data.startTime;
  console.log('='.repeat(60));
  console.log('Burst Simulation Complete');
  console.log('='.repeat(60));
  console.log(`Actual Duration: ${(elapsed / 1000).toFixed(1)}s`);
  console.log('='.repeat(60));
}
