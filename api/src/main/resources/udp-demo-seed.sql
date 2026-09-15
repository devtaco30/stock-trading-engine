-- fork1, Unit 3b — 로컬 3-JVM UDP 라이브 왕복 검증용 데모 시드.
-- account-worker의 application-udp.yml `account-worker.seed-accounts`와 accountId·stockCode가
-- 반드시 일치해야 한다(매수 90001, 매도 90002, 종목 A900110).
INSERT INTO users (id) VALUES (9001);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status) VALUES
(90001, 9001, 100000000, 0.40, 'ACTIVE'),
(90002, 9001, 100000000, 0.40, 'ACTIVE');

INSERT INTO stocks (stock_code, stock_name, mrkt_ctg) VALUES
('A900110', '데모전자', 'KOSDAQ');

INSERT INTO quotes (stock_code, current_price, previous_close, change_rate, open, high, low, volume, updated_at) VALUES
('A900110', 1003, 1003, 0.0, 941, 1007, 928, 343636, NOW());
