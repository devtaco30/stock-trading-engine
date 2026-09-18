-- 계좌 샤딩 조정 모드 검증용 데모 시드. account-worker의 application-shard-a.yml·-b.yml의
-- `account-worker.seed-accounts`와 accountId·stockCode가 반드시 일치해야 한다.
-- 매수 90001(슬롯 109, 앞 절반) / 매도 90005(슬롯 150, 뒤 절반) — 서로 다른 워커에 실린다.
INSERT INTO users (id) VALUES (9001);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status) VALUES
(90001, 9001, 100000000, 0.40, 'ACTIVE'),
(90005, 9001, 100000000, 0.40, 'ACTIVE');

INSERT INTO stocks (stock_code, stock_name, mrkt_ctg) VALUES
('A900110', '데모전자', 'KOSDAQ');

INSERT INTO quotes (stock_code, current_price, previous_close, change_rate, open, high, low, volume, updated_at) VALUES
('A900110', 1003, 1003, 0.0, 941, 1007, 928, 343636, NOW());
