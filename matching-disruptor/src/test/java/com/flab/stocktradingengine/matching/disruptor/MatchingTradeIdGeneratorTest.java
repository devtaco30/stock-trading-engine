package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.matching.disruptor.engine.MatchingTradeIdGenerator;

/**
 * v2 체결번호 발급기. 시계를 읽지 않고 (nodeId &lt;&lt; 53) | 카운터로 만든다(ADR-023 갱신 2c). 계좌 쪽
 * {@code AccountOrderIdGenerator}와 같은 형식이다.
 */
class MatchingTradeIdGeneratorTest {

    private static final int COUNTER_BITS = 53;

    @Test
    @DisplayName("첫 번호는 카운터 1이다(0은 발급하지 않는다)")
    void firstIdIsCounterOne() {
        MatchingTradeIdGenerator generator = new MatchingTradeIdGenerator(0);

        assertEquals(1L, generator.next());
        assertEquals(2L, generator.next());
    }

    @Test
    @DisplayName("같은 nodeId면 N번째 값이 항상 같다")
    void sameNodeSameSequence() {
        MatchingTradeIdGenerator first = new MatchingTradeIdGenerator(4);
        MatchingTradeIdGenerator second = new MatchingTradeIdGenerator(4);

        for (int i = 0; i < 100; i++) {
            assertEquals(first.next(), second.next());
        }
    }

    @Test
    @DisplayName("nodeId가 높은 비트에 들어가고 카운터가 낮은 비트에 들어간다")
    void nodeIdInHighBitsCounterInLowBits() {
        MatchingTradeIdGenerator generator = new MatchingTradeIdGenerator(4);

        long id = generator.next();

        assertEquals(4L, id >>> COUNTER_BITS);
        assertEquals(1L, id & ((1L << COUNTER_BITS) - 1));
    }

    @Test
    @DisplayName("nodeId가 다르면 값도 다르다")
    void differentNodeDifferentValue() {
        assertTrue(new MatchingTradeIdGenerator(3).next() != new MatchingTradeIdGenerator(4).next());
    }

    @Test
    @DisplayName("발급값은 항상 양수다")
    void alwaysPositive() {
        MatchingTradeIdGenerator generator = new MatchingTradeIdGenerator(1023);
        generator.restoreCounter((1L << COUNTER_BITS) - 3);

        for (int i = 0; i < 2; i++) {
            assertTrue(generator.next() > 0);
        }
    }

    @Test
    @DisplayName("nodeId가 범위(0~1023)를 벗어나면 가장자리 값으로 자른다")
    void nodeIdClamped() {
        assertEquals(0L, new MatchingTradeIdGenerator(-5).next() >>> COUNTER_BITS);
        assertEquals(1023L, new MatchingTradeIdGenerator(5000).next() >>> COUNTER_BITS);
    }

    @Test
    @DisplayName("counter()는 발급 횟수를 돌려주고 restoreCounter 뒤에는 그다음 값부터 이어 발급한다")
    void counterAndRestore() {
        MatchingTradeIdGenerator generator = new MatchingTradeIdGenerator(4);
        generator.next();
        generator.next();
        assertEquals(2L, generator.counter());

        MatchingTradeIdGenerator restored = new MatchingTradeIdGenerator(4);
        restored.restoreCounter(2L);

        assertEquals(generator.next(), restored.next());
    }
}
