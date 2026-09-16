import http from 'k6/http';
import { check } from 'k6';
import { SharedArray } from 'k6/data';
import exec from 'k6/execution';

// 39 설계(decision_records/v1-v2-e2e-measurement.md) 축2 — 결정론 페어 입력 파일을 그대로
// 순서대로 재생한다. HOT/랜덤 분산 없음 — 파일에 이미 계좌·종목·가격이 결정돼 있다.
const VERSION = __ENV.VERSION || 'v1'; // 'v1' | 'v2'
// e2e 계좌(2001~2100)는 user_id=2로 시드된다 — run-e2e-v1.sh/v2.sh가 심어두는 e2e-token-1이
// 그 userId로 매핑돼 있다(기존 loadtest-token-1은 user_id=1용이라 여기선 403이 난다).
const TOKEN = __ENV.TOKEN || 'e2e-token-1';
const BASE = __ENV.BASE || 'http://localhost:8080';
const VUS = parseInt(__ENV.VUS || '50');
const INPUT_FILE = __ENV.INPUT_FILE || './results/e2e-input.jsonl';
// 같은 입력 파일을 재기동 없이 여러 패스(워밍업+측정3) 연속 재생하므로, 원본 requestId를 그대로
// 쓰면 두 번째 패스부터 멱등 캐시가 재전송으로 오인해 전부 duplicate로 스킵된다(계좌·예약 반영이
// 안 되고 histogram에도 안 잡힘) — 패스마다 접미사를 붙여 서로 다른 요청으로 만든다.
const PASS = __ENV.PASS || 'default';
// A(용량 측정)는 비율만 보면 되므로 전체 10만 건을 다 안 쏴도 된다 — N으로 앞에서부터
// 잘라 쏘는 건수를 줄인다(2b 지시, 2026-09-16). 안 주면 파일 전체.
const N = __ENV.N ? parseInt(__ENV.N) : null;

const orders = new SharedArray('e2e-orders', function () {
  return open(INPUT_FILE)
    .split('\n')
    .filter((line) => line.trim().length > 0)
    .map((line) => JSON.parse(line));
});
const ITERATIONS = N ? Math.min(N, orders.length) : orders.length;

export const options = {
  scenarios: {
    replay: { executor: 'shared-iterations', vus: VUS, iterations: ITERATIONS, maxDuration: '600s' },
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
    requestId: `${order.requestId}-${PASS}`,
  });
  const res = http.post(`${BASE}/api/${VERSION}/orders/${path}`, body, {
    headers: { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' },
  });
  check(res, { 'status is 202': (r) => r.status === 202 });
}
