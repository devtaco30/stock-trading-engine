package com.flab.stocktradingengine.matching.disruptor.handler;

import com.lmax.disruptor.EventHandler;

import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.matching.disruptor.engine.OrderEvent;
import com.flab.stocktradingengine.matching.disruptor.journal.Journal;

/**
 * 링버퍼를 소비해 주문을 저널에 기록하는 핸들러.
 *
 * <h3>매칭보다 먼저 도는 이유</h3>
 * <p>{@code MatchingEngine} 에서 이 핸들러를 {@code handleEventsWith(journal).then(matcher)} 로 배선한다.
 * 매처는 이 핸들러가 기록을 마친 뒤에만 그 이벤트를 처리한다. 그래서 순서가 항상
 * "먼저 기록 → 그다음 매칭"이 되고, 기록되지 않은 주문이 체결되는 일이 없다.</p>
 *
 * <h3>슬롯을 비우지 않는 이유</h3>
 * <p>이 핸들러는 마지막 소비자가 아니다. 뒤이어 매처가 같은 슬롯을 읽어야 하므로
 * 여기서 {@code clear()} 를 호출하지 않는다. 슬롯 비우기는 마지막 소비자(매처)가 맡는다.</p>
 */
public class JournalEventHandler implements EventHandler<OrderEvent> {

    private final Journal journal;

    public JournalEventHandler(Journal journal) {
        this.journal = journal;
    }

    @Override
    public void onEvent(OrderEvent event, long sequence, boolean endOfBatch) {
        // JournaledOrder(core.wire)는 OrderEvent(매칭 코어 전용 가변 슬롯)를 모르므로 여기서 직접 옮겨 담는다.
        journal.append(new JournaledOrder(
            event.getType(), event.getOrderId(), event.getAccountId(), event.getStockCode(),
            event.getSide(), event.getPrice(), event.getQuantity(), event.getOrderAt()));
    }
}
