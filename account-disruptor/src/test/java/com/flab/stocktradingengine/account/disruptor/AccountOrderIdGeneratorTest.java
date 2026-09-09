package com.flab.stocktradingengine.account.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AccountOrderIdGeneratorTest {

    @Test
    @DisplayName("nodeId=0이면 1부터 순서대로 발급한다")
    void nodeId_0이면_1부터_순서대로() {
        AccountOrderIdGenerator generator = new AccountOrderIdGenerator(0L);

        assertEquals(1L, generator.next());
        assertEquals(2L, generator.next());
        assertEquals(3L, generator.next());
    }

    @Test
    @DisplayName("같은 nodeId면 같은 순번(N번째 next())의 값이 항상 같다(결정론)")
    void 같은_nodeId면_N번째_값이_항상_같다() {
        AccountOrderIdGenerator first = new AccountOrderIdGenerator(7L);
        AccountOrderIdGenerator second = new AccountOrderIdGenerator(7L);

        for (int i = 0; i < 5; i++) {
            assertEquals(first.next(), second.next());
        }
    }

    @Test
    @DisplayName("nodeId가 다르면 같은 순번이라도 값이 다르다(전역 유일성)")
    void nodeId가_다르면_값도_다르다() {
        AccountOrderIdGenerator node1 = new AccountOrderIdGenerator(1L);
        AccountOrderIdGenerator node2 = new AccountOrderIdGenerator(2L);

        assertNotEquals(node1.next(), node2.next());
    }

    @Test
    @DisplayName("발급값은 항상 양수다")
    void 발급값은_항상_양수() {
        AccountOrderIdGenerator generator = new AccountOrderIdGenerator(AccountOrderIdGenerator.MAX_NODE_ID);

        for (int i = 0; i < 5; i++) {
            assertTrue(generator.next() > 0);
        }
    }

    @Test
    @DisplayName("nodeId가 범위를 벗어나면 0 또는 최댓값으로 클램프한다")
    void nodeId_범위_벗어나면_클램프() {
        AccountOrderIdGenerator negative = new AccountOrderIdGenerator(-1L);
        AccountOrderIdGenerator zero = new AccountOrderIdGenerator(0L);
        assertEquals(zero.next(), negative.next()); // -1 → 0으로 클램프돼 nodeId=0과 같은 값

        AccountOrderIdGenerator overMax = new AccountOrderIdGenerator(AccountOrderIdGenerator.MAX_NODE_ID + 1);
        AccountOrderIdGenerator atMax = new AccountOrderIdGenerator(AccountOrderIdGenerator.MAX_NODE_ID);
        assertEquals(atMax.next(), overMax.next()); // 범위 초과 → MAX_NODE_ID로 클램프돼 같은 값
    }
}
