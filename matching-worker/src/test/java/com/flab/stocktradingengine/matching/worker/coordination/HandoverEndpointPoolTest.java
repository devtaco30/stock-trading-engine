package com.flab.stocktradingengine.matching.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

class HandoverEndpointPoolTest {

    private static final String SELF = "aeron:udp?endpoint=localhost:20020";
    private static final String PEER = "aeron:udp?endpoint=localhost:20021";

    @Test
    void 자기_주소는_풀에_있어도_Publication을_열_상대에서_빠진다() {
        HandoverEndpointPool pool = new HandoverEndpointPool(List.of(SELF, PEER), SELF);

        assertThat(pool.peers()).containsExactly(PEER);
        assertThat(pool.isOwn(SELF)).isTrue();
    }

    @Test
    void 풀에_없는_주소가_나오면_바로_실패한다() {
        HandoverEndpointPool pool = new HandoverEndpointPool(List.of(SELF, PEER), SELF);

        assertThat(pool.requireInPool(PEER)).isEqualTo(PEER);
        assertThatThrownBy(() -> pool.requireInPool("aeron:udp?endpoint=localhost:29999"))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("풀에 없는 주소");
    }

    @Test
    void 아홉_개_이상이면_기동을_막는다() {
        List<String> nine = IntStream.range(0, 9).mapToObj(i -> "aeron:udp?endpoint=localhost:" + (20020 + i)).toList();

        assertThatThrownBy(() -> new HandoverEndpointPool(nine, SELF))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("최대 8개");
    }

    @Test
    void 빈_주소와_중복_주소는_막는다() {
        assertThatThrownBy(() -> new HandoverEndpointPool(List.of(SELF, " "), SELF))
            .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> new HandoverEndpointPool(List.of(SELF, SELF), SELF))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("두 번");
    }
}
