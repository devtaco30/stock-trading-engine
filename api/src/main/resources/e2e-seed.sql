-- e2e 측정용 시드(e2e 프로파일 전용, generate-e2e-input.py 산출물). 재실행하면 이 파일도
-- 같이 다시 생성되므로 직접 손으로 고치지 말 것.
-- 계좌 2001~2050=매수자(50), 2051~2100=매도자(50). 종목은 stocks.sql/quotes.sql 공용.
-- 잔고·보유는 실제 생성된 주문에서 역산한 필요량 × 4패스(워밍업 1 + 측정 3).
-- 멱등: sql.init(mode=always)이 재기동마다 다시 돌아도 중복 INSERT로 실패하지 않는다.
INSERT INTO users (id)
SELECT 2 WHERE NOT EXISTS (SELECT 1 FROM users WHERE id = 2);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2001, 2, 287537476, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2001);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2002, 2, 287522644, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2002);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2003, 2, 287553708, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2003);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2004, 2, 287772948, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2004);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2005, 2, 287852028, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2005);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2006, 2, 284186256, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2006);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2007, 2, 282822556, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2007);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2008, 2, 282822556, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2008);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2009, 2, 282822556, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2009);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2010, 2, 282822556, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2010);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2011, 2, 282822556, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2011);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2012, 2, 282822556, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2012);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2013, 2, 282822556, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2013);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2014, 2, 282822556, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2014);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2015, 2, 282822556, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2015);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2016, 2, 282822556, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2016);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2017, 2, 282822556, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2017);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2018, 2, 282828272, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2018);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2019, 2, 282879276, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2019);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2020, 2, 282856288, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2020);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2021, 2, 282679808, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2021);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2022, 2, 282135408, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2022);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2023, 2, 281687780, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2023);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2024, 2, 281760940, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2024);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2025, 2, 281760940, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2025);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2026, 2, 281760940, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2026);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2027, 2, 281760940, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2027);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2028, 2, 281760940, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2028);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2029, 2, 281760940, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2029);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2030, 2, 281760940, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2030);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2031, 2, 281760940, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2031);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2032, 2, 281760940, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2032);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2033, 2, 281760940, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2033);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2034, 2, 281760940, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2034);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2035, 2, 281723632, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2035);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2036, 2, 281724120, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2036);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2037, 2, 281729924, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2037);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2038, 2, 282094364, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2038);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2039, 2, 285807364, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2039);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2040, 2, 285635904, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2040);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2041, 2, 287556104, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2041);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2042, 2, 287556104, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2042);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2043, 2, 287556104, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2043);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2044, 2, 287556104, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2044);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2045, 2, 287556104, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2045);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2046, 2, 287556104, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2046);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2047, 2, 287556104, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2047);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2048, 2, 287556104, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2048);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2049, 2, 287556104, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2049);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2050, 2, 287556104, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2050);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2051, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2051);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2052, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2052);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2053, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2053);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2054, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2054);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2055, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2055);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2056, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2056);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2057, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2057);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2058, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2058);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2059, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2059);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2060, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2060);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2061, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2061);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2062, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2062);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2063, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2063);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2064, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2064);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2065, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2065);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2066, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2066);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2067, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2067);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2068, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2068);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2069, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2069);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2070, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2070);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2071, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2071);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2072, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2072);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2073, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2073);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2074, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2074);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2075, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2075);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2076, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2076);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2077, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2077);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2078, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2078);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2079, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2079);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2080, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2080);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2081, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2081);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2082, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2082);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2083, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2083);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2084, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2084);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2085, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2085);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2086, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2086);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2087, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2087);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2088, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2088);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2089, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2089);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2090, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2090);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2091, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2091);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2092, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2092);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2093, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2093);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2094, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2094);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2095, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2095);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2096, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2096);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2097, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2097);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2098, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2098);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2099, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2099);

INSERT INTO accounts (account_id, user_id, balance, margin_rate, status)
SELECT 2100, 2, 0, 1.00, 'ACTIVE'
WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = 2100);

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 136, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 136, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 136, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 136, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 136, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 136, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 136, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 136, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 136, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 136, 0 FROM accounts a
WHERE a.account_id = 2051
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 136, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 136, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 136, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 136, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 136, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 136, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 136, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 136, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 136, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 136, 0 FROM accounts a
WHERE a.account_id = 2052
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 136, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 136, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 136, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 136, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 136, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 136, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 136, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 136, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 136, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 136, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2053
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 136, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 136, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 136, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 136, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 136, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 136, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 136, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 136, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 136, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 136, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2054
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 136, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 136, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 136, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 136, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 136, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 136, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 136, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 136, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 136, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 136, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2055
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 136, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 136, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 136, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 136, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 136, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 136, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 136, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 136, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 136, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 136, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2056
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 136, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 136, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 136, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 136, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 136, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 136, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 136, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 136, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 136, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 136, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2057
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 136, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 136, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 136, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 136, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 136, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 136, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 136, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 136, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 136, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 136, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2058
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 136, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 136, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 136, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 136, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 136, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 136, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 136, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 136, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 136, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 136, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2059
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 136, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 136, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 136, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 136, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 136, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 136, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 136, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 136, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 136, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 136, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2060
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 136, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 136, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 136, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 136, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 136, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 136, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 136, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 136, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 136, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 136, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2061
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 136, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 136, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 136, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 136, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 136, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 136, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 136, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 136, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 136, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 136, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2062
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 136, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 136, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 136, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 136, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 136, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 136, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 136, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 136, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 136, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 136, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2063
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 136, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 136, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 136, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 136, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 136, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 136, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 136, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 136, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 136, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 136, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2064
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 136, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 136, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 136, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 136, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 136, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 136, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 136, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 136, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 136, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 136, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2065
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 136, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 136, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 136, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 136, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 136, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 136, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 136, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 136, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 136, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 136, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2066
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 136, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 136, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 136, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 136, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 136, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 136, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 136, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 136, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 136, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 136, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2067
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 136, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 136, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 136, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 136, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 136, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 136, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 136, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 136, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 136, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 136, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2068
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 136, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 136, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 136, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 136, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 136, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 136, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 136, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 136, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 136, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 136, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2069
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 136, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 136, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 136, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 136, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 136, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 136, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 136, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 136, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 136, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 136, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2070
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 136, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 136, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 136, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 136, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 136, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 136, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 136, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 136, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 136, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 136, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2071
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 136, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 136, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 136, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 136, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 136, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 136, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 136, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 136, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 136, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 136, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2072
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 136, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 136, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 136, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 136, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 136, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 136, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 136, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 136, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 136, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 136, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2073
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 136, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 136, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 136, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 136, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 136, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 136, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 136, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 136, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 136, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 136, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2074
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 136, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 136, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 136, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 136, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 136, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 136, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 136, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 136, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 136, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 136, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2075
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 136, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 136, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 136, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 136, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 136, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 136, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 136, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 136, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 136, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 136, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2076
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 136, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 136, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 136, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 136, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 136, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 136, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 136, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 136, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 136, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 136, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2077
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 136, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 136, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 136, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 136, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 136, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 136, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 136, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 136, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 136, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 136, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2078
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 136, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 136, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 136, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 136, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 136, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 136, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 136, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 136, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 136, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 136, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2079
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 136, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 136, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 136, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 136, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 136, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 136, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 136, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 136, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 136, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 136, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2080
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 136, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 136, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 136, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 136, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 136, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 136, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 136, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 136, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 136, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 136, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2081
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 136, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 136, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 136, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 136, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 136, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 136, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 136, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 136, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 136, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 136, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2082
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 136, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 136, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 136, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 136, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 136, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 136, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 136, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 136, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 136, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 136, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2083
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 136, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 136, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 136, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 136, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 136, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 136, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 136, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 136, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 132, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 136, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 132, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 136, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2084
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 136, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 136, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 136, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 136, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 136, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 136, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 136, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 136, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 136, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 136, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 132, 0 FROM accounts a
WHERE a.account_id = 2085
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 136, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 132, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 136, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 136, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 136, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 136, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 136, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 136, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 132, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 136, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 136, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 136, 0 FROM accounts a
WHERE a.account_id = 2086
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 136, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 136, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 132, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 136, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 136, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 136, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 136, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 136, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 136, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 136, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 136, 0 FROM accounts a
WHERE a.account_id = 2087
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 136, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 136, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 136, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 132, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 136, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 136, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 136, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 132, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 136, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 136, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 136, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 136, 0 FROM accounts a
WHERE a.account_id = 2088
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 136, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 136, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 136, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 136, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 132, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 136, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 136, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 136, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 136, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 136, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 136, 0 FROM accounts a
WHERE a.account_id = 2089
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 136, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 136, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 136, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 136, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 136, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 132, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 136, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 136, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 136, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 136, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 136, 0 FROM accounts a
WHERE a.account_id = 2090
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 136, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 136, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 136, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 136, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 136, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 136, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 136, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 136, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 136, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 136, 0 FROM accounts a
WHERE a.account_id = 2091
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 136, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 136, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 136, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 136, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 136, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 136, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 136, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 136, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 136, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 136, 0 FROM accounts a
WHERE a.account_id = 2092
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 136, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 136, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 136, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 136, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 136, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 136, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 136, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 136, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 136, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 136, 0 FROM accounts a
WHERE a.account_id = 2093
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 136, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 136, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 136, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 136, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 136, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 136, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 136, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 136, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 136, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 136, 0 FROM accounts a
WHERE a.account_id = 2094
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 136, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 136, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 136, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 136, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 136, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 136, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 136, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 136, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 136, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 136, 0 FROM accounts a
WHERE a.account_id = 2095
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 136, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 136, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 136, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 136, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 136, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 136, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 136, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 136, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 136, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 136, 0 FROM accounts a
WHERE a.account_id = 2096
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 136, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 136, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 136, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 136, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 136, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 136, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 136, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 136, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 136, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 136, 0 FROM accounts a
WHERE a.account_id = 2097
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 136, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 136, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 136, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 136, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 136, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 136, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 136, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 136, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 136, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 136, 0 FROM accounts a
WHERE a.account_id = 2098
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 136, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 136, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 136, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 136, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 136, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 136, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 136, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 136, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 136, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 136, 0 FROM accounts a
WHERE a.account_id = 2099
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000020', 132, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000040', 132, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000050', 136, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000070', 132, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000080', 132, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000100', 136, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000120', 132, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000140', 132, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000150', 136, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000180', 132, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000210', 132, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000220', 136, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000230', 132, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000240', 132, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000250', 136, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000270', 132, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000300', 132, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000320', 136, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000370', 132, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000390', 132, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000400', 136, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000430', 132, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000440', 132, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A000480', 136, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900260', 132, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900270', 132, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900290', 136, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900300', 132, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900310', 132, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT a.id, 'A900340', 136, 0 FROM accounts a
WHERE a.account_id = 2100
AND NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = a.id AND h.stock_code = 'A900340');

