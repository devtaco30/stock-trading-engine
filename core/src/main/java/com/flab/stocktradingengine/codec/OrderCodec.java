package com.flab.stocktradingengine.codec;

import java.math.BigDecimal;
import java.time.Instant;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * Unit 4b — 주문을 Aeron 이 나를 수 있는 바이트로 바꾸는 수동 코덱.
 *
 * <p>Aeron 은 자바 객체가 아니라 바이트만 전송한다. 그래서 주문을 보낼 때 필드를 바이트 버퍼에
 * 직접 써 넣고(encode), 받을 때 다시 필드로 복원한다(decode). 디코딩 대상은 새 타입을 만들지 않고
 * 기존 불변 스냅샷 {@link JournaledOrder}(주문 필드를 그대로 담는 record)를 재사용한다.</p>
 *
 * <h3>메시지 레이아웃 (running offset, 앞에서 뒤로)</h3>
 * <p>맨 앞 1바이트가 {@link EventType} 를 구분한다. PLACE 는 전체 필드, CANCEL 은 orderId·stockCode 만 담는다.
 * 가변 길이인 stockCode 는 맨 뒤에 둬(Agrona {@code putStringAscii}: 4바이트 길이 + ASCII 바이트),
 * 뒤따르는 필드의 오프셋 계산이 필요 없게 한다.</p>
 * <pre>
 * PLACE : [type:1][orderId:8][accountId:8][side:1][priceUnscaled:8][priceScale:4]
 *         [quantity:4][orderAtEpochSecond:8][orderAtNano:4][stockCode:4+N]
 * CANCEL: [type:1][orderId:8][stockCode:4+N]
 * </pre>
 *
 * <h3>가정·한계 (블로그 감)</h3>
 * <ul>
 *   <li>가격 {@link BigDecimal} 은 (unscaledValue:long, scale:int) 로 담는다. 주가는 long 범위 안이라
 *       {@code longValueExact()} 가 안전하다. 범위를 넘는 값이면 예외로 드러난다(조용히 깎지 않는다).</li>
 *   <li>enum 은 {@code ordinal()} 로 담는다. enum 상수 순서를 바꾸면 기존 바이트 해석이 어긋난다 —
 *       스키마 진화가 필요해지면 SBE 로 옮긴다.</li>
 *   <li>버퍼는 기본(네이티브) 바이트 순서를 쓴다. Unit 4a 의 IPC(같은 머신)에선 문제없지만,
 *       UDP 로 머신 경계를 넘을 땐 양쪽 바이트 순서를 고정해야 한다(이후 단위).</li>
 * </ul>
 */
public final class OrderCodec {

    /**
     * 주문을 버퍼에 인코딩한다.
     *
     * @return 기록한 바이트 수(= Aeron {@code offer} 에 넘길 length)
     */
    public int encode(MutableDirectBuffer buffer, int offset, JournaledOrder order) {
        int position = offset;

        buffer.putByte(position, (byte) order.type().ordinal());
        position += Byte.BYTES;
        buffer.putLong(position, order.orderId());
        position += Long.BYTES;

        if (order.type() == EventType.CANCEL) {
            position += buffer.putStringAscii(position, order.stockCode());
            return position - offset;
        }

        buffer.putLong(position, order.accountId());
        position += Long.BYTES;
        buffer.putByte(position, (byte) order.side().ordinal());
        position += Byte.BYTES;

        BigDecimal price = order.price();
        long unscaledPrice = price.unscaledValue().longValueExact();
        buffer.putLong(position, unscaledPrice);
        position += Long.BYTES;
        buffer.putInt(position, price.scale());
        position += Integer.BYTES;

        buffer.putInt(position, order.quantity());
        position += Integer.BYTES;

        Instant orderAt = order.orderAt();
        buffer.putLong(position, orderAt.getEpochSecond());
        position += Long.BYTES;
        buffer.putInt(position, orderAt.getNano());
        position += Integer.BYTES;

        position += buffer.putStringAscii(position, order.stockCode());
        return position - offset;
    }

    /** 버퍼에서 주문 하나를 디코딩한다. */
    public JournaledOrder decode(DirectBuffer buffer, int offset) {
        int position = offset;

        EventType type = EventType.values()[buffer.getByte(position)];
        position += Byte.BYTES;
        long orderId = buffer.getLong(position);
        position += Long.BYTES;

        if (type == EventType.CANCEL) {
            String stockCode = buffer.getStringAscii(position);
            return new JournaledOrder(type, orderId, 0L, stockCode, null, null, 0, null);
        }

        long accountId = buffer.getLong(position);
        position += Long.BYTES;
        OrderSide side = OrderSide.values()[buffer.getByte(position)];
        position += Byte.BYTES;

        long unscaledPrice = buffer.getLong(position);
        position += Long.BYTES;
        int priceScale = buffer.getInt(position);
        position += Integer.BYTES;
        BigDecimal price = BigDecimal.valueOf(unscaledPrice, priceScale);

        int quantity = buffer.getInt(position);
        position += Integer.BYTES;

        long epochSecond = buffer.getLong(position);
        position += Long.BYTES;
        int nano = buffer.getInt(position);
        position += Integer.BYTES;
        Instant orderAt = Instant.ofEpochSecond(epochSecond, nano);

        String stockCode = buffer.getStringAscii(position);
        return new JournaledOrder(type, orderId, accountId, stockCode, side, price, quantity, orderAt);
    }
}
