package com.flab.stocktradingengine.matching.worker.handover;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HandoverRequestTest {

    @Test
    void 요청을_바이트로_바꿨다가_되돌리면_모든_칸이_같다() {
        HandoverRequest original = new HandoverRequest((6L << 53) | 7L, 12, "aeron:udp?endpoint=localhost:20731");

        byte[] wire = original.encode();
        HandoverRequest decoded = HandoverRequest.decode(wire, 0, wire.length);

        assertThat(decoded).isEqualTo(original);
    }
}
