package com.flab.stocktradingengine.trading.matching;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("OrderBookRegistry 단위 테스트")
class OrderBookRegistryTest {

    private OrderBookRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new OrderBookRegistry();
    }

    @Nested
    @DisplayName("getOrCreate")
    class GetOrCreate {

        @Test
        @DisplayName("처음 조회하면 새 OrderBook 반환")
        void 처음조회시_새_OrderBook() {
            OrderBook book = registry.getOrCreate("005930");

            assertThat(book).isNotNull();
        }

        @Test
        @DisplayName("같은 종목 두 번 조회하면 동일 인스턴스 반환")
        void 같은종목_동일_인스턴스() {
            OrderBook first  = registry.getOrCreate("005930");
            OrderBook second = registry.getOrCreate("005930");

            assertThat(first).isSameAs(second);
        }

        @Test
        @DisplayName("다른 종목은 서로 다른 인스턴스 반환")
        void 다른종목_다른_인스턴스() {
            OrderBook samsung = registry.getOrCreate("005930");
            OrderBook kakao   = registry.getOrCreate("035720");

            assertThat(samsung).isNotSameAs(kakao);
        }
    }

    @Nested
    @DisplayName("get")
    class Get {

        @Test
        @DisplayName("등록된 종목은 null 이 아닌 OrderBook 반환")
        void 등록된_종목_반환() {
            registry.getOrCreate("005930");

            assertThat(registry.get("005930")).isNotNull();
        }

        @Test
        @DisplayName("미등록 종목은 null 반환")
        void 미등록_종목_null() {
            assertThat(registry.get("999999")).isNull();
        }

        @Test
        @DisplayName("get은 getOrCreate와 동일 인스턴스 반환")
        void get_getOrCreate_동일_인스턴스() {
            OrderBook created = registry.getOrCreate("005930");

            assertThat(registry.get("005930")).isSameAs(created);
        }
    }
}
