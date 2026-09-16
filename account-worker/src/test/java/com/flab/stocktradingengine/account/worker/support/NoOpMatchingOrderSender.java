package com.flab.stocktradingengine.account.worker.support;

import java.math.BigDecimal;

import com.flab.stocktradingengine.account.disruptor.io.MatchingOrderSender;
import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 아무것도 하지 않는 {@link MatchingOrderSender} 테스트 더블(I2 발신측 U1). 계좌 복구·저널·정산
 * 같은 계좌 단독 기능만 보는 테스트가 실제 {@code AeronMatchingOrderSender}를 안 쓰게 하는 자리다
 * — 그 테스트들은 매칭 프로세스를 안 띄우므로 매칭 인테이크 채널에 아무도 안 붙어 있고, 진짜
 * sender를 쓰면 큐가 가득 찰 일은 없지만 {@code forwardPlace}가 부르는 발신마다 전용 스레드가
 * "연결 안 됨"을 계속 재시도하다가 컨텍스트 종료 시 드레인 타임아웃(5초)을 매번 다 채운다(D2가
 * 의도한 동작이지만 이 테스트들엔 무관한 비용이다).
 *
 * <h3>⚠️ 프로덕션 배선에 쓰지 말 것</h3>
 * <p>이 클래스는 {@code src/test}에만 있고 프로덕션 {@code MatchingOrderSenderConfig}는 참조하지
 * 않는다. 이 더블을 프로덕션에 쓰면 주문이 매칭에 영영 전달되지 않는데(계좌 예약만 남고) 아무
 * 에러도 안 난다 — 지금 고치는 중인 "발신하다 버리는" 버그와 정확히 같은 결과를 조용히 만든다.
 * 그래서 프로덕션 프로퍼티로 껐다 켰다 하는 스위치를 만들지 않는다(테스트 컨텍스트에만 등록).</p>
 */
public class NoOpMatchingOrderSender implements MatchingOrderSender {

    @Override
    public void forwardPlace(long orderId, long accountId, String stockCode, OrderSide side, BigDecimal price, int quantity) {
        // no-op — 이 테스트들은 매칭 전달 자체를 검증하지 않는다(AccountToMatchingForwardingIntegrationTest가 그 몫이다).
    }
}
