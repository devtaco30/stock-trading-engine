package com.flab.stocktradingengine.matching.disruptor;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;

import com.flab.stocktradingengine.trading.matching.OrderBook;

/**
 * Unit 0 스모크 테스트.
 *
 * <p>목적: {@code matching-disruptor} 모듈에서 Disruptor({@link RingBuffer})와
 * 기존 매칭 클래스({@link OrderBook})가 컴파일·런타임 classpath 에 모두 잡히는지
 * 실제 인스턴스 생성으로 증명한다. 파일 존재가 아니라 실행으로 확인한다.</p>
 *
 * <p>Unit 1 에서 실제 파이프라인 코드(OrderEvent·Feeder·Matcher)로 대체·삭제된다.</p>
 */
class DisruptorSmokeTest {

    @Test
    void 두_의존성이_classpath에_잡힌다() {
        // 기존 매칭 도메인 클래스 (trading 모듈) — 재사용 대상
        OrderBook orderBook = new OrderBook();
        assertNotNull(orderBook);

        // Disruptor 링버퍼 (com.lmax:disruptor) — bufferSize 는 2의 거듭제곱이어야 함
        EventFactory<long[]> eventFactory = () -> new long[1];
        RingBuffer<long[]> ringBuffer = RingBuffer.createSingleProducer(eventFactory, 1024);
        assertNotNull(ringBuffer);
    }
}
