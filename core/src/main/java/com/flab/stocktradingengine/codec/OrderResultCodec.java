package com.flab.stocktradingengine.codec;

import java.util.Optional;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * {@link OrderResultEntry}를 Aeron Archive 결과 스트림에 내보내기 전 바이트로 바꾸는 수동 코덱.
 * {@link AccountJournalEntryCodec}과 같은 결(고정 필드 먼저, 가변 문자열은 맨 뒤, 존재 여부는 flags
 * 바이트로 표시)을 따른다.
 *
 * <h3>메시지 레이아웃 (running offset, 앞에서 뒤로)</h3>
 * <pre>
 * [verdict:1][flags:1][accountId:8][orderId:8][epochMillis:8]
 *   (flags 의 REQUEST_ID 비트가 서면)     [requestId:4+N]
 *   (flags 의 REJECT_REASON 비트가 서면)  [rejectReason:4+M]
 * </pre>
 */
public final class OrderResultCodec {

    private static final byte REQUEST_ID_PRESENT = 0b0000_0001;
    private static final byte REJECT_REASON_PRESENT = 0b0000_0010;

    private static final int FIXED_LENGTH = Byte.BYTES + Byte.BYTES + Long.BYTES + Long.BYTES + Long.BYTES;

    /**
     * 결과 엔트리를 버퍼에 인코딩한다.
     *
     * @return 기록한 바이트 수(= Aeron {@code offer}에 넘길 length)
     */
    public int encode(MutableDirectBuffer buffer, int offset, OrderResultEntry entry) {
        int position = offset;

        buffer.putByte(position, (byte) entry.verdict().ordinal());
        position += Byte.BYTES;

        byte flags = flagsFor(entry);
        buffer.putByte(position, flags);
        position += Byte.BYTES;

        buffer.putLong(position, entry.accountId());
        position += Long.BYTES;
        buffer.putLong(position, entry.orderId());
        position += Long.BYTES;
        buffer.putLong(position, entry.epochMillis());
        position += Long.BYTES;

        if (entry.requestId() != null) {
            position += buffer.putStringAscii(position, entry.requestId());
        }
        if (entry.rejectReason() != null) {
            position += buffer.putStringAscii(position, entry.rejectReason());
        }
        return position - offset;
    }

    /**
     * content-poison 방어(구조 검증) — 길이·verdict 서수가 범위를 벗어나거나 버퍼가 잘려 있으면
     * 예외 대신 빈 값을 돌려준다. {@code length}는 이 프레임에 실제로 쓰인 바이트 수로, 이 경계를
     * 넘는 읽기는 전부 truncation으로 취급한다.
     */
    public Optional<OrderResultEntry> tryDecode(DirectBuffer buffer, int offset, int length) {
        try {
            int end = offset + length;
            int position = offset;

            if (position + FIXED_LENGTH > end) {
                return Optional.empty();
            }

            byte verdictOrdinal = buffer.getByte(position);
            if (verdictOrdinal < 0 || verdictOrdinal >= OrderVerdict.values().length) {
                return Optional.empty();
            }
            OrderVerdict verdict = OrderVerdict.values()[verdictOrdinal];
            position += Byte.BYTES;

            byte flags = buffer.getByte(position);
            position += Byte.BYTES;

            long accountId = buffer.getLong(position);
            position += Long.BYTES;
            long orderId = buffer.getLong(position);
            position += Long.BYTES;
            long epochMillis = buffer.getLong(position);
            position += Long.BYTES;

            String requestId = null;
            if ((flags & REQUEST_ID_PRESENT) != 0) {
                Optional<String> read = readString(buffer, position, end);
                if (read.isEmpty()) {
                    return Optional.empty();
                }
                requestId = read.get();
                position += Integer.BYTES + requestId.length();
            }

            String rejectReason = null;
            if ((flags & REJECT_REASON_PRESENT) != 0) {
                Optional<String> read = readString(buffer, position, end);
                if (read.isEmpty()) {
                    return Optional.empty();
                }
                rejectReason = read.get();
                position += Integer.BYTES + rejectReason.length();
            }

            return Optional.of(
                new OrderResultEntry(accountId, orderId, requestId, verdict, rejectReason, epochMillis));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private Optional<String> readString(DirectBuffer buffer, int position, int end) {
        if (position + Integer.BYTES > end) {
            return Optional.empty();
        }
        int stringLength = buffer.getInt(position);
        if (stringLength < 0 || position + Integer.BYTES + stringLength > end) {
            return Optional.empty();
        }
        return Optional.of(buffer.getStringAscii(position));
    }

    private byte flagsFor(OrderResultEntry entry) {
        byte flags = 0;
        if (entry.requestId() != null) {
            flags |= REQUEST_ID_PRESENT;
        }
        if (entry.rejectReason() != null) {
            flags |= REJECT_REASON_PRESENT;
        }
        return flags;
    }
}
