package com.flab.stocktradingengine.aeron;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class WorkerEndpointsTest {

    @Test
    @DisplayName("주문 주소와 체결 주소를 한 줄로 만들고 다시 읽어낸다")
    void 두_주소를_싣고_읽는다() {
        WorkerEndpoints endpoints = new WorkerEndpoints(
            "aeron:udp?endpoint=localhost:20040", "aeron:udp?endpoint=localhost:20060");

        WorkerEndpoints parsed = WorkerEndpoints.parse(endpoints.encode());

        assertEquals("aeron:udp?endpoint=localhost:20040", parsed.orderEndpoint());
        assertEquals("aeron:udp?endpoint=localhost:20060", parsed.fillEndpoint());
    }

    @Test
    @DisplayName("주소가 하나만 적힌 옛 값은 주문·체결 둘 다 그 주소로 읽는다")
    void 옛_형식은_한_주소를_둘로_읽는다() {
        WorkerEndpoints parsed = WorkerEndpoints.parse("aeron:udp?endpoint=localhost:20040");

        assertEquals("aeron:udp?endpoint=localhost:20040", parsed.orderEndpoint());
        assertEquals("aeron:udp?endpoint=localhost:20040", parsed.fillEndpoint());
    }

    @Test
    @DisplayName("Aeron 채널에 들어 있는 등호와 물음표를 그대로 보존한다")
    void 채널_문자열을_훼손하지_않는다() {
        String order = "aeron:udp?endpoint=localhost:20040|term-length=64k";
        String fill = "aeron:udp?endpoint=localhost:20060|term-length=64k";

        WorkerEndpoints parsed = WorkerEndpoints.parse(new WorkerEndpoints(order, fill).encode());

        assertEquals(order, parsed.orderEndpoint());
        assertEquals(fill, parsed.fillEndpoint());
    }

    @Test
    @DisplayName("빈 값은 읽을 수 없다")
    void 빈_값은_예외() {
        assertThrows(IllegalArgumentException.class, () -> WorkerEndpoints.parse(""));
    }
}
