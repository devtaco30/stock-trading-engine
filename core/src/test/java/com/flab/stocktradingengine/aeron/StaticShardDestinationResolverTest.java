package com.flab.stocktradingengine.aeron;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.ShardRoutingTable.ShardRange;

/**
 * 계좌 샤딩 U1 — {@link ShardRoutingTable}을 감싸 {@link AccountDestinationResolver} 인터페이스로
 * 노출하는 정적 구현 검증. 보내는 쪽 코드(U2)가 나중에 동적 구현(U5)으로 갈아탈 수 있도록 반환형을
 * {@code Optional}로 통일하는 것이 이 클래스의 유일한 책임이다.
 *
 * <p>{@link ShardRoutingTable}은 기동 시점에 슬롯 커버리지를 검증해(갭·중복 0) 실제로는 null도
 * 예외도 주지 않는 {@code final} 클래스다. 그래서 이 테스트는 정상 경로(경계 슬롯이 기대한
 * endpoint로 가는지)만 확인한다 — 방어 분기(null·예외 → empty)는 흉내낼 방법이 없어 구현부
 * ({@link StaticShardDestinationResolver})의 주석으로만 근거를 남긴다.</p>
 */
class StaticShardDestinationResolverTest {

    private static final String ENDPOINT_A = "aeron:udp?endpoint=localhost:6001";
    private static final String ENDPOINT_B = "aeron:udp?endpoint=localhost:6002";

    @Nested
    @DisplayName("정상 경로")
    class HappyPath {

        @Test
        void 슬롯_경계_계좌가_기대한_endpoint로_간다() {
            ShardRoutingTable table = new ShardRoutingTable(256, List.of(
                new ShardRange(ENDPOINT_A, 0, 127),
                new ShardRange(ENDPOINT_B, 128, 255)));
            StaticShardDestinationResolver resolver = new StaticShardDestinationResolver(table);

            long accountIdInSlot0 = firstAccountIdInSlot(table, 0);
            long accountIdInSlot255 = firstAccountIdInSlot(table, 255);

            assertEquals(Optional.of(ENDPOINT_A), resolver.orderEndpointFor(accountIdInSlot0));
            assertEquals(Optional.of(ENDPOINT_B), resolver.orderEndpointFor(accountIdInSlot255));
        }

        @Test
        void slotCount가_1이면_모든_계좌가_같은_endpoint를_받는다() {
            ShardRoutingTable table = new ShardRoutingTable(1, List.of(new ShardRange(ENDPOINT_A, 0, 0)));
            StaticShardDestinationResolver resolver = new StaticShardDestinationResolver(table);

            assertEquals(Optional.of(ENDPOINT_A), resolver.orderEndpointFor(0L));
            assertEquals(Optional.of(ENDPOINT_A), resolver.orderEndpointFor(Long.MAX_VALUE));
        }

        private long firstAccountIdInSlot(ShardRoutingTable table, int targetSlot) {
            for (long accountId = 0; accountId < 10_000; accountId++) {
                if (table.slotFor(accountId) == targetSlot) {
                    return accountId;
                }
            }
            throw new IllegalStateException("탐색 범위 내에 슬롯 " + targetSlot + "에 해당하는 accountId가 없다");
        }
    }
}
