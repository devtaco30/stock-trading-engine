-- 부하 테스트 시드 (loadtest 프로파일 전용). user 1 + 계좌 1001~1050. 종목은 stocks.sql/quotes.sql 사용.
-- 멱등: api 재기동으로 sql.init(mode=always)이 다시 돌아도 중복 INSERT로 실패하지 않는다.
INSERT INTO users (id)
SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM users WHERE id = 1);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT g, 1, 100000000000, 1.00, 'ACTIVE'
FROM generate_series(1001, 1050) AS g
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = g);
