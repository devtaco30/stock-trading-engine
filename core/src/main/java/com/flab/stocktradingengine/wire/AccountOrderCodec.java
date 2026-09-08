package com.flab.stocktradingengine.wire;

import java.math.BigDecimal;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * C5-1b — 매수·매도 주문을 Aeron 이 나를 바이트로 바꾸는 수동 코덱.
 *
 * <p>matching(wire)의 {@link OrderCodec}과 같은 결(가변길이 필드는 맨 뒤, enum은 ordinal,
 * 네이티브 바이트 순서, BigDecimal은 unscaledValue+scale) — 그대로 재사용하지 않고 새로 둔 이유는
 * 계좌 인테이크가 requestId(재전송 멱등키)를 추가로 실어야 하는데, 매칭 쪽 {@code JournaledOrder}에는
 * 그 필드가 없어서다.</p>
 *
 * <h3>메시지 레이아웃 (running offset, 앞에서 뒤로)</h3>
 * <p>orderId는 없다(C5-2a) — 발신자(게이트웨이)가 주문 신원을 정하지 않고, 계좌 워커가 requestId
 * 첫 접수 시점에 직접 발급한다. 맨 앞 1바이트는 {@link OrderSide}(BUY/SELL)를 구분한다 — 매칭의
 * {@link EventType}(PLACE/CANCEL)과는 다른 명령 종류다. 매도는 담보가 보유 수량이라 price가 없다
 * ({@code AccountEngine.publishSell} 참고) — 그래서 BUY만 가격 필드를 싣는다. stockCode·requestId는
 * 둘 다 가변 길이라 맨 뒤에 순서대로 두고(Agrona {@code putStringAscii}: 4바이트 길이 + ASCII 바이트),
 * 디코딩 때 각자의 길이 접두어로 다음 필드 오프셋을 계산한다.</p>
 * <pre>
 * BUY : [type:1][accountId:8][priceUnscaled:8][priceScale:4][quantity:4][stockCode:4+N][requestId:4+M]
 * SELL: [type:1][accountId:8][quantity:4][stockCode:4+N][requestId:4+M]
 * </pre>
 */
public final class AccountOrderCodec {

    /**
     * 주문을 버퍼에 인코딩한다.
     *
     * @return 기록한 바이트 수(= Aeron {@code offer}에 넘길 length)
     */
    public int encode(MutableDirectBuffer buffer, int offset, DecodedAccountOrder order) {
        int position = offset;

        buffer.putByte(position, (byte) order.type().ordinal());
        position += Byte.BYTES;
        buffer.putLong(position, order.accountId());
        position += Long.BYTES;

        if (order.type() == OrderSide.BUY) {
            BigDecimal price = order.price();
            long unscaledPrice = price.unscaledValue().longValueExact();
            buffer.putLong(position, unscaledPrice);
            position += Long.BYTES;
            buffer.putInt(position, price.scale());
            position += Integer.BYTES;
        }

        buffer.putInt(position, order.quantity());
        position += Integer.BYTES;

        position += buffer.putStringAscii(position, order.stockCode());
        position += buffer.putStringAscii(position, order.requestId());
        return position - offset;
    }

    /** 버퍼에서 주문 하나를 디코딩한다. */
    public DecodedAccountOrder decode(DirectBuffer buffer, int offset) {
        int position = offset;

        OrderSide type = OrderSide.values()[buffer.getByte(position)];
        position += Byte.BYTES;
        long accountId = buffer.getLong(position);
        position += Long.BYTES;

        BigDecimal price = null;
        if (type == OrderSide.BUY) {
            long unscaledPrice = buffer.getLong(position);
            position += Long.BYTES;
            int priceScale = buffer.getInt(position);
            position += Integer.BYTES;
            price = BigDecimal.valueOf(unscaledPrice, priceScale);
        }

        int quantity = buffer.getInt(position);
        position += Integer.BYTES;

        int stockCodeLength = buffer.getInt(position);
        String stockCode = buffer.getStringAscii(position);
        position += Integer.BYTES + stockCodeLength;

        String requestId = buffer.getStringAscii(position);

        return new DecodedAccountOrder(type, accountId, stockCode, price, quantity, requestId);
    }
}
