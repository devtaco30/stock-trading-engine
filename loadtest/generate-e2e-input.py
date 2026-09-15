#!/usr/bin/env python3
"""39 설계(decision_records/v1-v2-e2e-measurement.md) 전 과정(e2e) 측정용 결정론 페어 입력 생성기.

종목 30개마다 같은 가격·같은 수량(quantity=1)의 매수·매도를 짝수 개씩 만든다 — 어떤 순서로
도착하든 그 종목의 매수·매도가 전량 소진돼 v1·v2의 체결 건수가 항상 같아진다(도착 순서 의존
제거, decision_records/v1-v2-e2e-measurement.md 근거).

재실행하면 항상 같은 결과가 나온다(파일 안 순서 섞기만 고정 시드 random을 쓴다 — 계좌·종목
배정 자체는 라운드로빈이라 결정론적이다).

산출물:
  loadtest/results/e2e-input.jsonl        (dc가 리플레이) — gitignore 대상, 매번 재생성
  api/src/main/resources/e2e-seed.sql     (v1 시드, 커밋 대상)
  account-worker/src/main/resources/application-e2e.yml (v2 시드, 커밋 대상)
잔고·보유는 이 스크립트가 생성한 주문에서 실제 필요량을 역산해 4패스(워밍업 1 + 측정 3,
재기동 없이 연속 재생) 분량으로 채운다 — 짐작치가 아니라 생성 결과 집계값이라 거부 0이 보장된다.
"""
import json
import random
from pathlib import Path

SEED = 20260916
BUYER_IDS = list(range(2001, 2051))   # 매수자 50 — 기존 HTTP 처리량 테스트(1001~1050)와 안 겹침
SELLER_IDS = list(range(2051, 2101))  # 매도자 50
TOTAL_ORDERS = 100_000                # 매수 5만 + 매도 5만
PASSES = 4                            # 워밍업 1 + 측정 3, 재기동 없이 연속 재생(측정 스펙 갱신분)

# market/src/main/resources/quotes.sql의 previousClose를 그대로 가져왔다(가격제한폭 검증을
# 항상 0% 이탈로 통과시키기 위해 — OrderApiService.validatePriceBandLimit 확인 완료).
# 데모 프로파일이 쓰는 A900110은 겹치지 않게 뺐다.
STOCKS = [
    ("A900270", 168), ("A900260", 1597), ("A900290", 3845), ("A900300", 137),
    ("A900310", 598), ("A900340", 720), ("A000020", 6170), ("A000040", 423),
    ("A000050", 10690), ("A000070", 65500), ("A000080", 18090), ("A000100", 109200),
    ("A000120", 145700), ("A000140", 9600), ("A000150", 918000), ("A000180", 1557),
    ("A000210", 47250), ("A000220", 4385), ("A000230", 11660), ("A000240", 29950),
    ("A000250", 510000), ("A000270", 161800), ("A000300", 4200), ("A000320", 24050),
    ("A000370", 7320), ("A000390", 10610), ("A000400", 1794), ("A000430", 4110),
    ("A000440", 16400), ("A000480", 4825),
]

REPO_ROOT = Path(__file__).resolve().parents[1]


def orders_per_stock(total_pairs, num_stocks):
    """total_pairs를 num_stocks에 최대한 고르게 나눈다(나머지는 앞쪽 종목이 하나씩 더 가짐)."""
    base = total_pairs // num_stocks
    remainder = total_pairs - base * num_stocks
    return [base + 1 if i < remainder else base for i in range(num_stocks)]


def build_orders():
    total_pairs = TOTAL_ORDERS // 2  # 종목별 매수 건수 = 매도 건수 → 짝이 항상 맞음
    counts = orders_per_stock(total_pairs, len(STOCKS))

    buy_orders = []
    sell_orders = []
    buyer_cursor = 0
    seller_cursor = 0
    for (stock_code, price), count in zip(STOCKS, counts):
        for _ in range(count):
            buyer_id = BUYER_IDS[buyer_cursor % len(BUYER_IDS)]
            buyer_cursor += 1
            buy_orders.append({"side": "BUY", "accountId": buyer_id, "stockCode": stock_code,
                                "price": price, "quantity": 1})

            seller_id = SELLER_IDS[seller_cursor % len(SELLER_IDS)]
            seller_cursor += 1
            sell_orders.append({"side": "SELL", "accountId": seller_id, "stockCode": stock_code,
                                 "price": price, "quantity": 1})

    orders = buy_orders + sell_orders
    random.seed(SEED)
    random.shuffle(orders)  # 파일 안 순서도 재현 가능하게 섞어 실제 혼재 트래픽처럼 보이게 한다

    for i, order in enumerate(orders, start=1):
        order["requestId"] = f"e2e-{i:06d}"

    return orders


def write_jsonl(orders):
    out_dir = REPO_ROOT / "loadtest" / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "e2e-input.jsonl"
    with out_path.open("w") as f:
        for order in orders:
            f.write(json.dumps(order, ensure_ascii=False) + "\n")
    return out_path


def aggregate_buyer_cost(orders):
    """accountId -> 그 계좌가 낼 모든 매수 주문의 price*quantity 합(marginRate 1.00 전제)."""
    totals = {}
    for order in orders:
        if order["side"] != "BUY":
            continue
        totals[order["accountId"]] = totals.get(order["accountId"], 0) + order["price"] * order["quantity"]
    return totals


def aggregate_seller_holdings(orders):
    """(accountId, stockCode) -> 그 종목 매도 주문 수량 합."""
    totals = {}
    for order in orders:
        if order["side"] != "SELL":
            continue
        key = (order["accountId"], order["stockCode"])
        totals[key] = totals.get(key, 0) + order["quantity"]
    return totals


def write_v1_seed(buyer_cost, seller_holdings):
    margin_rate = "1.00"
    lines = [
        "-- e2e 측정용 시드(e2e 프로파일 전용, generate-e2e-input.py 산출물). 재실행하면 이 파일도",
        "-- 같이 다시 생성되므로 직접 손으로 고치지 말 것.",
        "-- 계좌 2001~2050=매수자(50), 2051~2100=매도자(50). 종목은 stocks.sql/quotes.sql 공용.",
        "-- 잔고·보유는 실제 생성된 주문에서 역산한 필요량 × 4패스(워밍업 1 + 측정 3).",
        "-- 멱등: sql.init(mode=always)이 재기동마다 다시 돌아도 중복 INSERT로 실패하지 않는다.",
        "INSERT INTO users (id)",
        "SELECT 2 WHERE NOT EXISTS (SELECT 1 FROM users WHERE id = 2);",
        "",
    ]
    for buyer_id in BUYER_IDS:
        cost = buyer_cost.get(buyer_id, 0)
        balance = cost * PASSES
        lines.append(
            f"INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)\n"
            f"SELECT {buyer_id}, 2, {balance}, {margin_rate}, 'ACTIVE'\n"
            f"WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = {buyer_id});\n"
        )
    for seller_id in SELLER_IDS:
        lines.append(
            f"INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)\n"
            f"SELECT {seller_id}, 2, 0, {margin_rate}, 'ACTIVE'\n"
            f"WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = {seller_id});\n"
        )
    for (account_id, stock_code), qty in sorted(seller_holdings.items()):
        holding_qty = qty * PASSES
        lines.append(
            f"INSERT INTO holdings (account_id, stock_code, quantity, average_price)\n"
            f"SELECT {account_id}, '{stock_code}', {holding_qty}, 0\n"
            f"WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = {account_id} "
            f"AND h.stock_code = '{stock_code}');\n"
        )
    out_path = REPO_ROOT / "api" / "src" / "main" / "resources" / "e2e-seed.sql"
    out_path.write_text("\n".join(lines) + "\n")
    return out_path


def write_v2_seed(buyer_cost, seller_holdings):
    lines = [
        "# e2e 측정용 v2 계좌 시드(generate-e2e-input.py 산출물). 재실행하면 이 파일도 같이",
        "# 다시 생성되므로 직접 손으로 고치지 말 것. api의 e2e-seed.sql과 같은 계좌 집합·필요량.",
        "# --spring.profiles.active=udp,e2e로 켠다. udp 프로파일의 데모 계좌(90001/90002)는",
        "# 이 목록이 같은 인덱스를 덮어써 대체된다.",
        "account-worker:",
        "  metrics:",
        "    enabled: true",
        "  seed-accounts:",
    ]
    for buyer_id in BUYER_IDS:
        cost = buyer_cost.get(buyer_id, 0)
        balance = cost * PASSES
        lines.append(f"    - account-id: {buyer_id}")
        lines.append(f"      balance: {balance}")
        lines.append("      margin-rate: 1.00")

    holdings_by_seller = {}
    for (account_id, stock_code), qty in seller_holdings.items():
        holdings_by_seller.setdefault(account_id, {})[stock_code] = qty * PASSES

    for seller_id in SELLER_IDS:
        lines.append(f"    - account-id: {seller_id}")
        lines.append("      balance: 0")
        lines.append("      margin-rate: 1.00")
        holdings = holdings_by_seller.get(seller_id, {})
        if holdings:
            lines.append("      holdings:")
            for stock_code, qty in sorted(holdings.items()):
                lines.append(f"        {stock_code}: {qty}")

    out_path = REPO_ROOT / "account-worker" / "src" / "main" / "resources" / "application-e2e.yml"
    out_path.write_text("\n".join(lines) + "\n")
    return out_path


def main():
    orders = build_orders()
    jsonl_path = write_jsonl(orders)
    buyer_cost = aggregate_buyer_cost(orders)
    seller_holdings = aggregate_seller_holdings(orders)
    sql_path = write_v1_seed(buyer_cost, seller_holdings)
    yaml_path = write_v2_seed(buyer_cost, seller_holdings)

    buy_count = sum(1 for o in orders if o["side"] == "BUY")
    sell_count = sum(1 for o in orders if o["side"] == "SELL")
    print(f"주문 {len(orders)}건 생성(매수 {buy_count} / 매도 {sell_count}) -> {jsonl_path}")
    print(f"v1 시드({len(BUYER_IDS) + len(SELLER_IDS)}계좌 + holdings {len(seller_holdings)}행) -> {sql_path}")
    print(f"v2 시드 -> {yaml_path}")


if __name__ == "__main__":
    main()
