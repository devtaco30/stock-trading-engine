import http from 'k6/http';
import { check } from 'k6';

// v1 docs/loadtest/buy.js를 v2 게이트웨이(/api/v2/orders/buy)용으로 복사·수정.
// HOT=1: 모든 주문이 계좌 1001 → 단일 계좌 엔진 내 직렬 처리
// HOT=0: 계좌 1001~1050 분산 → 같은 계좌 엔진 안이지만 여러 계좌에 걸쳐 검증
const HOT = __ENV.HOT === '1';
const TOKEN = __ENV.TOKEN || 'loadtest-token-1';
const BASE = __ENV.BASE || 'http://localhost:8080';
const N = parseInt(__ENV.N || '3000');
const VUS = parseInt(__ENV.VUS || '50');

export const options = {
  scenarios: {
    load: { executor: 'shared-iterations', vus: VUS, iterations: N, maxDuration: '180s' },
  },
  summaryTrendStats: ['avg', 'p(95)', 'p(99)', 'max'],
};

export default function () {
  const accountId = HOT ? 1001 : (1001 + Math.floor(Math.random() * 50));
  // v2는 requestId가 필수다(서버 UUID 생성 fallback 없음, fork5 결정 ③) — 매 요청 유일한 값을 직접 채운다.
  const requestId = `${__VU}-${__ITER}-${Date.now()}`;
  const body = JSON.stringify({
    accountId: accountId,
    stockCode: 'A900110',
    orderType: 'LIMIT',
    price: 1003,
    quantity: 1,
    requestId: requestId,
  });
  const res = http.post(`${BASE}/api/v2/orders/buy`, body, {
    headers: { 'Authorization': `Bearer ${TOKEN}`, 'Content-Type': 'application/json' },
  });
  check(res, { 'status is 202': (r) => r.status === 202 });
}
