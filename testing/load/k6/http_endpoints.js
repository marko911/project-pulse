// k6 HTTP Endpoints Load Test
// Project Pulse - Milestone 4: Performance & WebSocket Scale
//
// Tests REST API endpoint performance including health checks,
// correctness status, manifest queries, and watermark monitoring.
//
// Usage:
//   k6 run http_endpoints.js                           # Smoke test
//   k6 run -e SCENARIO=load http_endpoints.js          # Load test
//   k6 run -e SCENARIO=stress http_endpoints.js        # Stress test
//   k6 run -e HTTP_URL=http://prod:8080 http_endpoints.js  # Custom URL

import http from 'k6/http';
import { check, group, sleep } from 'k6';
import { Counter, Trend, Rate } from 'k6/metrics';
import { config } from './config.js';

// Custom metrics
const httpRequests = new Counter('http_requests_total');
const httpErrors = new Counter('http_errors_total');
const healthLatency = new Trend('http_health_latency_ms');
const readyLatency = new Trend('http_ready_latency_ms');
const statusLatency = new Trend('http_correctness_status_latency_ms');
const manifestLatency = new Trend('http_manifest_latency_ms');
const watermarkLatency = new Trend('http_watermark_latency_ms');
const haltsLatency = new Trend('http_halts_latency_ms');
const gapsLatency = new Trend('http_gaps_latency_ms');
const httpErrorRate = new Rate('http_error_rate');

// Determine scenario from environment
const scenarioName = __ENV.SCENARIO || 'smoke';
const scenario = config.scenarios[scenarioName];

// Configure k6 options based on scenario
export const options = {
  scenarios: {
    http_endpoints: scenario.stages
      ? {
          executor: 'ramping-vus',
          startVUs: 0,
          stages: scenario.stages,
          gracefulRampDown: '30s',
        }
      : {
          executor: 'constant-vus',
          vus: scenario.vus,
          duration: scenario.duration,
        },
  },
  thresholds: {
    http_health_latency_ms: ['p(99)<50'],
    http_ready_latency_ms: ['p(99)<100'],
    http_correctness_status_latency_ms: ['p(99)<200'],
    http_manifest_latency_ms: ['p(99)<500'],
    http_error_rate: [`rate<${config.targets.errorRate}`],
  },
};

const baseUrl = config.httpUrl;
const chains = config.chains;

export default function () {
  // Health and readiness checks (most frequent)
  group('health_checks', function () {
    // Health endpoint
    let res = http.get(`${baseUrl}/health`);
    httpRequests.add(1);
    healthLatency.add(res.timings.duration);

    let passed = check(res, {
      'health status is 200': (r) => r.status === 200,
      'health response is ok': (r) => {
        try {
          const body = JSON.parse(r.body);
          return body.status === 'ok' || body.status === 'healthy';
        } catch {
          return false;
        }
      },
    });

    if (!passed) {
      httpErrors.add(1);
      httpErrorRate.add(1);
    } else {
      httpErrorRate.add(0);
    }

    // Ready endpoint
    res = http.get(`${baseUrl}/ready`);
    httpRequests.add(1);
    readyLatency.add(res.timings.duration);

    passed = check(res, {
      'ready status is 200 or 503': (r) => r.status === 200 || r.status === 503,
    });

    if (!passed) {
      httpErrors.add(1);
      httpErrorRate.add(1);
    } else {
      httpErrorRate.add(0);
    }
  });

  // Correctness API endpoints
  group('correctness_api', function () {
    // Correctness status
    let res = http.get(`${baseUrl}/api/v1/correctness/status`);
    httpRequests.add(1);
    statusLatency.add(res.timings.duration);

    let passed = check(res, {
      'correctness status is 200': (r) => r.status === 200,
      'correctness status has valid response': (r) => {
        try {
          const body = JSON.parse(r.body);
          return body.hasOwnProperty('status');
        } catch {
          return false;
        }
      },
    });

    if (!passed) {
      httpErrors.add(1);
      httpErrorRate.add(1);
    } else {
      httpErrorRate.add(0);
    }

    // Watermark endpoint
    res = http.get(`${baseUrl}/api/v1/correctness/watermark`);
    httpRequests.add(1);
    watermarkLatency.add(res.timings.duration);

    passed = check(res, {
      'watermark status is 200': (r) => r.status === 200,
    });

    if (!passed) {
      httpErrors.add(1);
      httpErrorRate.add(1);
    } else {
      httpErrorRate.add(0);
    }

    // Halts endpoint
    res = http.get(`${baseUrl}/api/v1/correctness/halts`);
    httpRequests.add(1);
    haltsLatency.add(res.timings.duration);

    passed = check(res, {
      'halts status is 200': (r) => r.status === 200,
      'halts response is array': (r) => {
        try {
          const body = JSON.parse(r.body);
          return Array.isArray(body.halts) || Array.isArray(body);
        } catch {
          return false;
        }
      },
    });

    if (!passed) {
      httpErrors.add(1);
      httpErrorRate.add(1);
    } else {
      httpErrorRate.add(0);
    }

    // Gaps endpoint
    res = http.get(`${baseUrl}/api/v1/correctness/gaps`);
    httpRequests.add(1);
    gapsLatency.add(res.timings.duration);

    passed = check(res, {
      'gaps status is 200': (r) => r.status === 200,
    });

    if (!passed) {
      httpErrors.add(1);
      httpErrorRate.add(1);
    } else {
      httpErrorRate.add(0);
    }
  });

  // Manifest queries (less frequent, heavier)
  if (Math.random() < 0.3) {
    group('manifest_queries', function () {
      // Pick a random chain
      const chain = chains[Math.floor(Math.random() * chains.length)];

      // Get manifests for chain
      let res = http.get(`${baseUrl}/api/v1/manifests/${chain}`);
      httpRequests.add(1);
      manifestLatency.add(res.timings.duration);

      let passed = check(res, {
        'manifest list status is valid': (r) =>
          r.status === 200 || r.status === 404 || r.status === 503,
      });

      if (!passed) {
        httpErrors.add(1);
        httpErrorRate.add(1);
      } else {
        httpErrorRate.add(0);
      }

      // Get specific block manifest (random block in recent range)
      const blockNum = 18000000 + Math.floor(Math.random() * 1000000);
      res = http.get(`${baseUrl}/api/v1/manifests/${chain}/${blockNum}`);
      httpRequests.add(1);
      manifestLatency.add(res.timings.duration);

      passed = check(res, {
        'manifest block status is valid': (r) =>
          r.status === 200 || r.status === 404 || r.status === 503,
      });

      if (!passed) {
        httpErrors.add(1);
        httpErrorRate.add(1);
      } else {
        httpErrorRate.add(0);
      }

      // Get mismatches (data integrity check)
      res = http.get(`${baseUrl}/api/v1/manifests/mismatches`);
      httpRequests.add(1);
      manifestLatency.add(res.timings.duration);

      passed = check(res, {
        'mismatches status is valid': (r) =>
          r.status === 200 || r.status === 404 || r.status === 503,
      });

      if (!passed) {
        httpErrors.add(1);
        httpErrorRate.add(1);
      } else {
        httpErrorRate.add(0);
      }
    });
  }

  // Random sleep to simulate realistic traffic pattern
  sleep(0.5 + Math.random() * 1.5);
}

// Teardown - log final metrics
export function teardown(data) {
  console.log('HTTP Endpoints Load Test Complete');
  console.log(`Scenario: ${scenarioName}`);
}
