package com.flab.stocktradingengine.codec;

import java.math.BigDecimal;
import java.util.Optional;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Unit 1 — 체결(fill) 하나를 Aeron 이 나를 수 있는 바이트로 바꾸는 수동 코덱.
 *
 * <p>Aeron 은 자바 객체가 아니라 바이트만 전송한다. 그래서 체결을 보낼 때 필드를 바이트 버퍼에
 * 직접 써 넣고(encode), 받을 때 다시 필드로 복원한다(decode). 디코딩 대상은 불변 스냅샷
 * {@link FilledTrade} 다. {@code OrderCodec} 을 미러한다.</p>
 *
 * <h3>메시지 레이아웃 (running offset, 앞에서 뒤로)</h3>
 * <p>체결은 단일 메시지 타입이라 {@code OrderCodec} 의 PLACE/CANCEL 같은 type 구분 바이트가 없다.
 * 가변 길이인 stockCode 는 맨 뒤에 둬(Agrona {@code putStringAscii}: 4바이트 길이 + ASCII 바이트),
 * 뒤따르는 필드의 오프셋 계산이 필요 없게 한다.</p>
 * <pre>
 * [tradeId:8][buyOrderId:8][buyAccountId:8][sellOrderId:8][sellAccountId:8]
 * [filledQuantity:4][priceUnscaled:8][priceScale:4][stockCode:4+N]
 * </pre>
 *
 * <h3>가정·한계</h3>
 * <ul>
 *   <li>가격 {@link BigDecimal} 은 (unscaledValue:long, scale:int) 로 담는다. 주가는 long 범위 안이라
 *       {@code longValueExact()} 가 안전하다. 범위를 넘는 값이면 예외로 드러난다(조용히 깎지 않는다).</li>
 *   <li>버퍼는 기본(네이티브) 바이트 순서를 쓴다. IPC(같은 머신)에선 문제없지만,
 *       UDP 로 머신 경계를 넘을 땐 양쪽 바이트 순서를 고정해야 한다(C5-4).</li>
 * </ul>
 */
public final class FillCodec {

    /**
     * 체결을 버퍼에 인코딩한다.
     *
     * @return 기록한 바이트 수(= Aeron {@code offer} 에 넘길 length)
     */
    public int encode(MutableDirectBuffer buffer, int offset, FilledTrade fill) {
        int position = offset;

        buffer.putLong(position, fill.tradeId());
        position += Long.BYTES;
        buffer.putLong(position, fill.buyOrderId());
        position += Long.BYTES;
        buffer.putLong(position, fill.buyAccountId());
        position += Long.BYTES;
        buffer.putLong(position, fill.sellOrderId());
        position += Long.BYTES;
        buffer.putLong(position, fill.sellAccountId());
        position += Long.BYTES;

        buffer.putInt(position, fill.filledQuantity());
        position += Integer.BYTES;

        BigDecimal matchPrice = fill.matchPrice();
        long unscaledPrice = matchPrice.unscaledValue().longValueExact();
        buffer.putLong(position, unscaledPrice);
        position += Long.BYTES;
        buffer.putInt(position, matchPrice.scale());
        position += Integer.BYTES;

        position += buffer.putStringAscii(position, fill.stockCode());
        return position - offset;
    }

    /** 버퍼에서 체결 하나를 디코딩한다. */
    public FilledTrade decode(DirectBuffer buffer, int offset) {
        int position = offset;

        long tradeId = buffer.getLong(position);
        position += Long.BYTES;
        long buyOrderId = buffer.getLong(position);
        position += Long.BYTES;
        long buyAccountId = buffer.getLong(position);
        position += Long.BYTES;
        long sellOrderId = buffer.getLong(position);
        position += Long.BYTES;
        long sellAccountId = buffer.getLong(position);
        position += Long.BYTES;

        int filledQuantity = buffer.getInt(position);
        position += Integer.BYTES;

        long unscaledPrice = buffer.getLong(position);
        position += Long.BYTES;
        int priceScale = buffer.getInt(position);
        position += Integer.BYTES;
        BigDecimal matchPrice = BigDecimal.valueOf(unscaledPrice, priceScale);

        String stockCode = buffer.getStringAscii(position);
        return new FilledTrade(
            tradeId, stockCode, buyOrderId, buyAccountId, sellOrderId, sellAccountId,
            filledQuantity, matchPrice);
    }

    /**
     * content-poison 방어(1층) — decode 가 던지게 두지 않고 무효면 예외 대신 빈 값을 돌려준다.
     *
     * <p>type 구분 바이트가 없어 {@code OrderCodec} 같은 ordinal 범위 검증은 해당 없다. 방어는
     * 얇은 catch 가 최후 안전망(Agrona 경계 검사가 손상 바이트에 던지면 흡수)이고, 더해
     * {@code filledQuantity < 0} 은 명백한 손상이라 예방적으로 걸러낸다. 예외를 제어 흐름으로
     * 쓰지 않는다 — 도메인 검증(수량·가격 타당성)은 계좌 핸들러 몫이고 여기선 구조/명백 손상만 본다.</p>
     */
    public Optional<FilledTrade> tryDecode(DirectBuffer buffer, int offset) {
        try {
            FilledTrade fill = decode(buffer, offset);
            if (fill.filledQuantity() < 0) {
                return Optional.empty();
            }
            return Optional.of(fill);
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }
}
