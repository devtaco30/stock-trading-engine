package com.flab.stocktradingengine.matching.worker.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import io.aeron.driver.MediaDriver;

/** 드라이버 Context를 만드는 코드를 메서드로 뺀 뒤에도 디렉터리와 삭제 설정이 그대로인지 확인한다. */
class MatchingOrderIntakeMediaDriverContextTest {

    @Test
    void 통신용_디렉터리를_쓰고_시작과_종료_때_지운다() {
        MediaDriver.Context context = MatchingOrderIntakeConfig.mediaDriverContext("/tmp/aeron-test-dir");

        assertEquals("/tmp/aeron-test-dir", context.aeronDirectoryName());
        assertTrue(context.dirDeleteOnStart());
        assertTrue(context.dirDeleteOnShutdown());
    }
}
