import http from 'k6/http';
import { check } from 'k6';
import { SharedArray } from 'k6/data';
import exec from 'k6/execution';

// 39 설계(decision_records/v1-v2-e2e-measurement.md) 축2 — 결정론 페어 입력 파일을 그대로
// 순서대로 재생한다. HOT/랜덤 분산 없음 — 파일에 이미 계좌·종목·가격이 결정돼 있다.
const VERSION = __ENV.VERSION || 'v1'; // 'v1' | 'v2'
const TOKEN = __ENV.TOKEN || 'loadtest-token-1';
const BASE = __ENV.BASE || 'http://localhost:8080';
const VUS = parseInt(__ENV.VUS || '50');
const INPUT_FILE = __ENV.INPUT_FILE || './results/e2e-input.jsonl';

const orders = new SharedArray('e2e-orders', function () {
  return open(INPUT_FILE)
    .split('\n')
    .filter((line) => line.trim().length > 0)
    .map((line) => JSON.parse(line));
});

export const options = {
  scenarios: {
    replay: { executor: 'shared-iterations', vus: VUS, iterations: orders.length, maxDuration: '600s' },
  },
  summaryTrendStats: ['avg', 'p(95)', 'p(99)', 'max'],
};

export default function () {
  // shared-iterations는 VU마다 __ITER가 따로 세지므로, 파일 전체를 정확히 한 번씩 순회하려면
  // 테스트 전체 기준 누적 순번(iterationInTest)으로 인덱싱해야 한다.
  const order = orders[exec.scenario.iterationInTest];
  const path = order.side === 'BUY' ? 'buy' : 'sell';
  const body = JSON.stringify({
    accountId: order.accountId,
    stockCode: order.stockCode,
    orderType: 'LIMIT',
    price: order.price,
    quantity: order.quantity,
    requestId: order.requestId,
  });
  const res = http.post(`${BASE}/api/${VERSION}/orders/${path}`, body, {
    headers: { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' },
  });
  check(res, { 'status is 202': (r) => r.status === 202 });
}
