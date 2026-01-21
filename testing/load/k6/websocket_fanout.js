import ws from 'k6/ws';
import { check, sleep } from 'k6';
import { Counter, Trend, Rate, Gauge } from 'k6/metrics';
import { config, generateEvent } from './config.js';

const wsConnections = new Counter('ws_connections_total');
const wsConnectionErrors = new Counter('ws_connection_errors_total');
const wsMessagesReceived = new Counter('ws_messages_received_total');
const wsMessagesSent = new Counter('ws_messages_sent_total');
const wsEventLatency = new Trend('ws_event_latency_ms');
const wsSubscriptionLatency = new Trend('ws_subscription_latency_ms');
const wsActiveConnections = new Gauge('ws_active_connections');
const wsErrorRate = new Rate('ws_error_rate');

const scenarioName = __ENV.SCENARIO || 'smoke';
const scenario = config.scenarios[scenarioName];

export const options = {
  scenarios: {
    websocket_fanout: scenario.stages
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
    ws_event_latency_ms: [`p(99)<${config.targets.p99Latency}`],
    ws_subscription_latency_ms: ['p(95)<100'],
    ws_error_rate: [`rate<${config.targets.errorRate}`],
    ws_connection_errors_total: ['count<10'],
  },
};

export default function () {
  const wsUrl = config.wsUrl;
  let clientId = null;
  let subscriptionId = null;
  let eventTimestamps = {};

  const res = ws.connect(wsUrl, {}, function (socket) {
    wsConnections.add(1);
    wsActiveConnections.add(1);

    socket.on('open', function () {
    });

    socket.on('message', function (data) {
      wsMessagesReceived.add(1);

      try {
        const msg = JSON.parse(data);

        switch (msg.type) {
          case 'connected':
            clientId = msg.client_id;
            check(msg, {
              'received client_id': (m) => m.client_id && m.client_id.length > 0,
            });

            const subscribeStart = Date.now();
            const filter = getRandomFilter();
            socket.send(
              JSON.stringify({
                type: 'subscribe',
                data: filter,
              })
            );
            wsMessagesSent.add(1);
            eventTimestamps['subscribe'] = subscribeStart;
            break;

          case 'subscribed':
            subscriptionId = msg.data.subscription_id;
            const subscribeLatency = Date.now() - eventTimestamps['subscribe'];
            wsSubscriptionLatency.add(subscribeLatency);

            check(msg, {
              'subscription created': (m) => m.data && m.data.subscription_id,
            });

            socket.send(JSON.stringify({ type: 'list_subscriptions', data: {} }));
            wsMessagesSent.add(1);
            break;

          case 'subscriptions':
            check(msg, {
              'subscriptions listed': (m) => m.data && Array.isArray(m.data.subscriptions),
              'has our subscription': (m) =>
                m.data.subscriptions.some((s) => s.id === subscriptionId),
            });
            break;

          case 'event':
            const eventId = msg.data.event_id;
            if (eventTimestamps[eventId]) {
              const latency = Date.now() - eventTimestamps[eventId];
              wsEventLatency.add(latency);
              delete eventTimestamps[eventId];
            }

            check(msg, {
              'event has required fields': (m) =>
                m.data.event_id && m.data.chain && m.data.event_type,
            });
            break;

          case 'pong':
            break;

          case 'error':
            wsErrorRate.add(1);
            console.error(`WebSocket error: ${msg.data.code} - ${msg.data.message}`);
            break;

          default:
            console.log(`Unknown message type: ${msg.type}`);
        }
      } catch (e) {
        wsErrorRate.add(1);
        console.error(`Failed to parse message: ${e.message}`);
      }
    });

    socket.on('error', function (e) {
      wsConnectionErrors.add(1);
      wsErrorRate.add(1);
      console.error(`WebSocket error: ${e.error()}`);
    });

    socket.on('close', function () {
      wsActiveConnections.add(-1);
    });

    socket.setInterval(function () {
      socket.send(JSON.stringify({ type: 'ping', data: {} }));
      wsMessagesSent.add(1);
    }, 30000);

    socket.setInterval(function () {
      if (subscriptionId && Math.random() < 0.1) {
        socket.send(
          JSON.stringify({
            type: 'unsubscribe',
            data: { subscription_id: subscriptionId },
          })
        );
        wsMessagesSent.add(1);

        socket.setTimeout(function () {
          const newFilter = getRandomFilter();
          eventTimestamps['subscribe'] = Date.now();
          socket.send(
            JSON.stringify({
              type: 'subscribe',
              data: newFilter,
            })
          );
          wsMessagesSent.add(1);
        }, 100);
      }
    }, 10000);

    socket.setTimeout(function () {
      if (subscriptionId) {
        socket.send(
          JSON.stringify({
            type: 'unsubscribe',
            data: { subscription_id: subscriptionId },
          })
        );
        wsMessagesSent.add(1);
      }
      socket.close();
    }, getDuration());
  });

  check(res, {
    'WebSocket connection successful': (r) => r && r.status === 101,
  });

  if (!res || res.status !== 101) {
    wsConnectionErrors.add(1);
    wsErrorRate.add(1);
  }
}

function getRandomFilter() {
  const complexity = Math.random();
  let filter;

  if (complexity < 0.5) {
    filter = { ...config.filters.simple };
  } else if (complexity < 0.8) {
    filter = { ...config.filters.moderate };
  } else {
    filter = { ...config.filters.complex };
  }

  filter.ttl_seconds = 3600;

  return filter;
}

function getDuration() {
  const baseDuration = parseInt(scenario.duration) || 60000;
  return baseDuration * (0.8 + Math.random() * 0.4);
}

export function teardown(data) {
  console.log('WebSocket Fanout Load Test Complete');
  console.log(`Scenario: ${scenarioName}`);
}
