package com.flab.stocktradingengine.matching.worker.config;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.aeron.ExclusivePublication;

/**
 * close() 도중 하나가 예외를 던져도 나머지 Publication은 전부 닫히는지 검증한다
 * (Cursor 1차 리뷰 ② — forEach는 예외 전파 시 중단돼 나머지가 안 닫혔다).
 */
class MatchingFillPublicationsTest {

    @Test
    void 하나가_close_중_예외를_던져도_나머지_전부_close된다() {
        ExclusivePublication first = mock(ExclusivePublication.class);
        ExclusivePublication throwing = mock(ExclusivePublication.class);
        ExclusivePublication last = mock(ExclusivePublication.class);
        doThrow(new RuntimeException("close 실패")).when(throwing).close();

        Map<String, ExclusivePublication> byEndpoint = new LinkedHashMap<>();
        byEndpoint.put("ep-1", first);
        byEndpoint.put("ep-2", throwing);
        byEndpoint.put("ep-3", last);

        MatchingFillPublications publications = new MatchingFillPublications(byEndpoint);
        publications.close();

        verify(first).close();
        verify(throwing).close();
        verify(last).close();
    }
}
