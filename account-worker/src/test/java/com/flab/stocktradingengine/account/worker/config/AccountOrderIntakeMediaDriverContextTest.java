package com.flab.stocktradingengine.account.worker.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import io.aeron.driver.MediaDriver;

import com.flab.stocktradingengine.support.IdleStrategies;
import com.flab.stocktradingengine.support.IdleStrategies.Mode;

/** 드라이버 Context를 만드는 코드를 메서드로 뺀 뒤에도 디렉터리와 삭제 설정이 그대로인지 확인한다. */
class AccountOrderIntakeMediaDriverContextTest {

    @Test
    void 통신용_디렉터리를_쓰고_시작과_종료_때_지운다() {
        MediaDriver.Context context = AccountOrderIntakeConfig.mediaDriverContext("/tmp/aeron-test-dir", 0);

        assertEquals("/tmp/aeron-test-dir", context.aeronDirectoryName());
        assertTrue(context.dirDeleteOnStart());
        assertTrue(context.dirDeleteOnShutdown());
    }

    @Test
    void ipc_term_버퍼_길이가_0보다_크면_드라이버에_넘긴다() {
        MediaDriver.Context context = AccountOrderIntakeConfig.mediaDriverContext("/tmp/aeron-test-dir", 65536);

        assertEquals(65536, context.ipcTermBufferLength());
    }

    @Test
    void DEFAULT와_THREADS_모드에서는_sender와_receiver의_쉬는_방식을_설정하지_않는다() {
        for (Mode mode : new Mode[] {Mode.DEFAULT, Mode.THREADS}) {
            MediaDriver.Context context = AccountOrderIntakeConfig.mediaDriverContext("/tmp/aeron-test-dir", 0, mode);

            assertNull(context.senderIdleStrategy(), mode.name());
            assertNull(context.receiverIdleStrategy(), mode.name());
        }
    }

    @Test
    void DRIVER_모드에서는_sender와_receiver에_park_상한을_줄인_전략을_따로_넣는다() {
        MediaDriver.Context context = AccountOrderIntakeConfig.mediaDriverContext("/tmp/aeron-test-dir", 0, Mode.DRIVER);

        assertTrue(context.senderIdleStrategy() instanceof IdleStrategies.ShortParkBackoffIdleStrategy);
        assertTrue(context.receiverIdleStrategy() instanceof IdleStrategies.ShortParkBackoffIdleStrategy);
        assertNotSame(context.senderIdleStrategy(), context.receiverIdleStrategy());
        assertNull(context.conductorIdleStrategy(), "conductor는 바꾸지 않는다");
    }
}
