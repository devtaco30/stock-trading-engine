package com.flab.stocktradingengine.matching.worker.handover;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.MatchingSlotTarget;
import com.flab.stocktradingengine.matching.worker.coordination.HandoverEndpointPool;

import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;

/** 이전 담당과 연결되지 않은 동안에는 같은 요청을 다시 보내지도 않는다(offer로 마감까지 coordinator thread를 붙잡지 않는다). */
class HandoverRequesterResendTest {

    private static final String OWN = "aeron:udp?endpoint=localhost:20021";
    private static final String PREVIOUS_OWNER = "aeron:udp?endpoint=localhost:20020";

    @Test
    void 연결되지_않은_동안_다시_보내라고_하면_offer_없이_실패하고_연결되면_보낸다() {
        ConcurrentPublication publication = mock(ConcurrentPublication.class);
        when(publication.isConnected()).thenReturn(true);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(100L);
        Aeron aeron = mock(Aeron.class);
        when(aeron.addPublication(anyString(), anyInt())).thenReturn(publication);
        HandoverEndpointPool pool = HandoverEndpointPool.requireForCoordination(List.of(OWN, PREVIOUS_OWNER), OWN);
        HandoverRequester requester = new HandoverRequester(aeron, OWN, pool,
            slot -> Optional.of(new MatchingSlotTarget(PREVIOUS_OWNER, true)), 1L);

        Optional<Long> transferId = requester.request(5);
        assertThat(transferId).isPresent();
        verify(publication, times(1)).offer(any(DirectBuffer.class), anyInt(), anyInt());

        when(publication.isConnected()).thenReturn(false);
        assertThat(requester.resend(5)).as("연결되지 않았으면 다시 보내지 않고 실패").isFalse();
        verify(publication, times(1)).offer(any(DirectBuffer.class), anyInt(), anyInt());

        when(publication.isConnected()).thenReturn(true);
        assertThat(requester.resend(5)).as("연결되면 같은 요청을 다시 보낸다").isTrue();
        verify(publication, times(2)).offer(any(DirectBuffer.class), anyInt(), anyInt());
        requester.close();
    }
}
