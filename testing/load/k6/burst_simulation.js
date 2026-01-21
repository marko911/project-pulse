import ws from 'k6/ws';
import http from 'k6/http';
import { check, sleep, group } from 'k6';
import { Counter, Trend, Rate, Gauge } from 'k6/metrics';
import { config, generateEvent, randomAccount } from './config.js';

const burstEventsReceived = new Counter('burst_events_received_total');
const burstEventLatency = new Trend('burst_event_latency_ms');
const burstDroppedEvents = new Counter('burst_dropped_events_total');
const burstRecoveryTime = new Trend('burst_recovery_time_ms');
const burstErrorRate = new Rate('burst_error_rate');
const currentBurstPhase = new Gauge('current_burst_phase');

const burstRatio = parseInt(__ENV.BURST_RATIO) || 10;
const burstDuration = parseInt(__ENV.BURST_DURATION) || 10;
const normalDuration = parseInt(__ENV.NORMAL_DURATION) || 30;
const totalCycles = parseInt(__ENV.TOTAL_CYCLES) || 5;

export const options = {
  scenarios: {
    websocket_receivers: {
      executor: 'constant-vus',
      vus: 100,
      duration: `${(burstDuration + normalDuration) * totalCycles + 60}s`,
      exec: 'websocketReceiver',
    },
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
    burst_error_rate: ['rate<0.01'],
    burst_recovery_time_ms: ['avg<5000'],
  },
};

let burstStartTime = null;
let inBurst = false;

export function websocketReceiver() {
  const wsUrl = config.wsUrl;
  let eventCount = 0;
  let lastEventTime = Date.now();

  const res = ws.connect(wsUrl, {}, function (socket) {
    socket.on('open', function () {
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

          if (inBurst && msg.data && msg.data.timestamp) {
            const eventTs = new Date(msg.data.timestamp).getTime();
            const latency = now - eventTs;
            burstEventLatency.add(latency);
          }

          const gap = now - lastEventTime;
          if (gap > 1000) {
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

    socket.setInterval(function () {
      socket.send(JSON.stringify({ type: 'ping', data: {} }));
    }, 30000);

    const duration = (burstDuration + normalDuration) * totalCycles * 1000 + 30000;
    socket.setTimeout(function () {
      socket.close();
    }, duration);
  });

  check(res, {
    'WebSocket connected': (r) => r && r.status === 101,
  });
}

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

  sleep(1);
}

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

export function teardown(data) {
  const elapsed = Date.now() - data.startTime;
  console.log('='.repeat(60));
  console.log('Burst Simulation Complete');
  console.log('='.repeat(60));
  console.log(`Actual Duration: ${(elapsed / 1000).toFixed(1)}s`);
  console.log('='.repeat(60));
}
