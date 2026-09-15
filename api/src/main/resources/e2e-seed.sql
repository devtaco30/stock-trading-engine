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
SELECT 2051, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000050', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000100', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000150', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000220', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000250', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000320', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000400', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A900270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A900290', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2051, 'A900340', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2051 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000050', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000100', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000150', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000220', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000250', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000320', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000400', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A900270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A900300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2052, 'A900340', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2052 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000020', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000050', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000100', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000150', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000220', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000250', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000320', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000430', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A900270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A900300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2053, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2053 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000020', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000070', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000100', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000150', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000220', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000250', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000320', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000430', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A900270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A900300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2054, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2054 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000020', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000070', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000120', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000150', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000220', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000250', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000370', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000430', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A900270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A900300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2055, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2055 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000020', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000070', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000120', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000180', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000220', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000250', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000370', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000430', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A900270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A900300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2056, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2056 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000020', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000070', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000120', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000180', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000230', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000370', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000430', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A900270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A900300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2057, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2057 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000020', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000070', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000120', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000180', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000230', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000370', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000430', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A900270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A900300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2058, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2058 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000020', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000070', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000120', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000180', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000230', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000370', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000430', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A900270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A900300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2059, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2059 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000020', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000070', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000120', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000180', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000230', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000370', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000430', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A900270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A900300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2060, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2060 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000020', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000070', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000120', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000180', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000230', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000370', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000430', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A900270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A900300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2061, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2061 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000020', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000070', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000120', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000180', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000230', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000370', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000430', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A900270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A900300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2062, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2062 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000020', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000070', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000120', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000180', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000230', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000370', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000430', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A900270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A900300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2063, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2063 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000020', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000070', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000120', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000180', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000230', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000370', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000430', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A900270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A900300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2064, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2064 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000020', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000070', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000120', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000180', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000230', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000370', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000430', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A900270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A900300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2065, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2065 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000020', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000070', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000120', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000180', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000230', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000370', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000430', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A900270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A900300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2066, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2066 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000020', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000070', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000120', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000180', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000230', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000370', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000430', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A900270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A900300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2067, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2067 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000020', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000070', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000120', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000180', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000230', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000370', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000430', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A900260', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A900300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2068, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2068 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000020', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000070', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000120', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000180', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000230', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000370', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000440', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A900260', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A900310', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2069, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2069 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000040', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000070', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000120', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000180', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000230', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000370', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000440', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A900260', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A900310', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2070, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2070 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000040', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000080', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000120', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000180', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000230', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000390', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000440', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A900260', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A900310', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2071, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2071 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000040', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000080', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000140', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000180', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000230', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000270', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000390', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000440', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A900260', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A900310', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2072, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2072 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000040', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000080', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000140', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000210', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000230', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000390', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000440', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A900260', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A900310', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2073, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2073 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000040', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000080', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000140', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000210', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000240', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000390', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000440', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A900260', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A900310', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2074, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2074 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000040', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000080', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000140', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000210', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000240', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000390', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000440', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A900260', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A900310', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2075, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2075 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000040', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000080', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000140', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000210', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000240', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000390', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000440', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A900260', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A900310', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2076, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2076 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000040', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000080', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000140', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000210', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000240', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000390', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000440', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A900260', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A900310', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2077, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2077 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000040', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000080', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000140', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000210', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000240', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000390', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000440', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A900260', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A900310', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2078, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2078 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000040', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000080', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000140', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000210', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000240', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000390', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000440', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A900260', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A900310', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2079, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2079 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000040', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000080', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000140', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000210', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000240', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000390', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000440', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A900260', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A900310', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2080, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2080 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000040', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000080', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000140', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000210', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000240', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000390', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000440', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A900260', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A900310', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2081, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2081 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000040', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000080', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000140', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000210', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000240', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000390', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000440', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A900260', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A900310', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2082, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2082 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000040', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000080', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000140', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000210', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000240', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000390', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000440', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A900260', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A900310', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2083, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2083 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000040', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000080', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000140', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000210', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000240', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000390', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000440', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A000480', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A900260', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A900290', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A900310', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2084, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2084 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000040', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000080', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000140', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000210', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000240', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000390', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A000480', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A900290', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A900310', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2085, 'A900340', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2085 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000040', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000050', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000080', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000140', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000210', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000240', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000390', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000400', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A000480', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A900290', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2086, 'A900340', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2086 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000050', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000080', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000100', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000140', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000210', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000240', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000400', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A000480', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A900290', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2087, 'A900340', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2087 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000050', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000100', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000140', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000150', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000210', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000240', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000300', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000320', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000400', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A000480', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A900290', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2088, 'A900340', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2088 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000050', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000100', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000150', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000210', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000220', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000240', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000320', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000400', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A000480', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A900290', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2089, 'A900340', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2089 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000050', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000100', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000150', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000220', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000240', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000250', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000320', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000400', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A000480', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A900290', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2090, 'A900340', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2090 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000050', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000100', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000150', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000220', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000250', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000320', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000400', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A000480', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A900290', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2091, 'A900340', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2091 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000050', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000100', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000150', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000220', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000250', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000320', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000400', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A000480', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A900290', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2092, 'A900340', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2092 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000050', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000100', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000150', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000220', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000250', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000320', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000400', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A000480', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A900290', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2093, 'A900340', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2093 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000050', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000100', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000150', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000220', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000250', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000320', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000400', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A000480', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A900290', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2094, 'A900340', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2094 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000050', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000100', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000150', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000220', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000250', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000320', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000400', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A000480', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A900290', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2095, 'A900340', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2095 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000050', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000100', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000150', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000220', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000250', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000320', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000400', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A000480', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A900290', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2096, 'A900340', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2096 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000050', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000100', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000150', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000220', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000250', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000320', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000400', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A000480', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A900290', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2097, 'A900340', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2097 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000050', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000100', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000150', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000220', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000250', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000320', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000400', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A000480', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A900290', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2098, 'A900340', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2098 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000050', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000100', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000150', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000220', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000250', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000320', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000400', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A000480', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A900290', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2099, 'A900340', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2099 AND h.stock_code = 'A900340');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000020', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000020');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000040', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000040');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000050', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000050');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000070', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000070');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000080', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000080');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000100', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000100');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000120', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000120');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000140', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000140');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000150', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000150');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000180', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000180');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000210', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000210');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000220', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000220');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000230', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000230');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000240', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000240');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000250', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000250');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000320', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000320');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000370', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000370');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000390', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000390');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000400', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000400');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000430', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000430');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000440', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000440');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A000480', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A000480');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A900260', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A900260');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A900270', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A900270');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A900290', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A900290');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A900300', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A900300');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A900310', 132, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A900310');

INSERT INTO holdings (account_id, stock_code, quantity, average_price)
SELECT 2100, 'A900340', 136, 0
WHERE NOT EXISTS (SELECT 1 FROM holdings h WHERE h.account_id = 2100 AND h.stock_code = 'A900340');

