package com.flab.stocktradingengine.aeron;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.aeron.ShardRoutingTable.ShardRange;

/**
 * fork3, Unit 1 — accountId를 정적 샤드 endpoint로 결정론적으로 매핑하는 라우팅 테이블 검증.
 *
 * <p>단일 writer 정합의 전제는 슬롯 소유가 유일하다는 것이므로, 기동 시 슬롯 커버리지(갭·중복 0)를
 * 검증하는 부분이 핵심이다. 그 외엔 같은 accountId가 항상 같은 endpoint로 가는지(결정론), 서로 다른
 * 슬롯 범위의 accountId들이 실제로 서로 다른 endpoint로 갈리는지(분배)를 확인한다.</p>
 */
class ShardRoutingTableTest {

    private static final String ENDPOINT_A = "aeron:udp?endpoint=localhost:6001";
    private static final String ENDPOINT_B = "aeron:udp?endpoint=localhost:6002";

    @Nested
    @DisplayName("결정론·분배")
    class RoutingBehavior {

        @Test
        void 같은_accountId는_항상_같은_endpoint로_간다() {
            ShardRoutingTable table = new ShardRoutingTable(256, List.of(
                new ShardRange(ENDPOINT_A, 0, 127),
                new ShardRange(ENDPOINT_B, 128, 255)));

            String first = table.endpointFor(90001L);
            String second = table.endpointFor(90001L);
            String third = table.endpointFor(90001L);

            assertEquals(first, second);
            assertEquals(first, third);
        }

        @Test
        void 서로_다른_슬롯_범위로_매핑되는_accountId들은_서로_다른_endpoint로_간다() {
            ShardRoutingTable table = new ShardRoutingTable(2, List.of(
                new ShardRange(ENDPOINT_A, 0, 0),
                new ShardRange(ENDPOINT_B, 1, 1)));

            long accountIdInSlot0 = firstAccountIdInSlot(table, 0);
            long accountIdInSlot1 = firstAccountIdInSlot(table, 1);

            assertEquals(ENDPOINT_A, table.endpointFor(accountIdInSlot0));
            assertEquals(ENDPOINT_B, table.endpointFor(accountIdInSlot1));
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

    @Nested
    @DisplayName("슬롯 커버리지 검증(기동 fail-fast)")
    class CoverageValidation {

        @Test
        void 갭이_있으면_기동_실패한다() {
            assertThrows(IllegalStateException.class, () -> new ShardRoutingTable(256, List.of(
                new ShardRange(ENDPOINT_A, 0, 100),
                new ShardRange(ENDPOINT_B, 150, 255))));
        }

        @Test
        void 범위가_겹쳐_중복되면_기동_실패한다() {
            assertThrows(IllegalStateException.class, () -> new ShardRoutingTable(256, List.of(
                new ShardRange(ENDPOINT_A, 0, 150),
                new ShardRange(ENDPOINT_B, 100, 255))));
        }

        @Test
        void 완전히_커버되면_기동_성공한다() {
            ShardRoutingTable table = assertDoesNotThrow(() -> new ShardRoutingTable(256, List.of(
                new ShardRange(ENDPOINT_A, 0, 127),
                new ShardRange(ENDPOINT_B, 128, 255))));

            String endpoint = table.endpointFor(90001L);

            assertTrue(Set.of(ENDPOINT_A, ENDPOINT_B).contains(endpoint), "매핑된 endpoint는 설정된 둘 중 하나여야 한다");
        }

        @Test
        void shards가_비어있으면_기동_실패한다() {
            assertThrows(IllegalStateException.class, () -> new ShardRoutingTable(256, List.of()));
        }

        @Test
        void slotCount가_0이면_기동_실패한다() {
            assertThrows(IllegalStateException.class, () -> new ShardRoutingTable(0, List.of(
                new ShardRange(ENDPOINT_A, 0, 0))));
        }

        @Test
        void 슬롯_범위가_slotCount를_벗어나면_기동_실패한다() {
            assertThrows(IllegalStateException.class, () -> new ShardRoutingTable(256, List.of(
                new ShardRange(ENDPOINT_A, 0, 255),
                new ShardRange(ENDPOINT_B, 256, 300))));
        }
    }

    @Nested
    @DisplayName("경계 조건")
    class BoundaryConditions {

        @Test
        void slotCount가_1이면_모든_계좌가_단일_endpoint로_간다() {
            ShardRoutingTable table = new ShardRoutingTable(1, List.of(new ShardRange(ENDPOINT_A, 0, 0)));

            assertEquals(ENDPOINT_A, table.endpointFor(0L));
            assertEquals(ENDPOINT_A, table.endpointFor(Long.MAX_VALUE));
            assertEquals(ENDPOINT_A, table.endpointFor(Long.MIN_VALUE));
            assertEquals(ENDPOINT_A, table.endpointFor(-1L));
        }

        @Test
        void 음수_accountId도_유효한_슬롯으로_방어된다() {
            ShardRoutingTable table = new ShardRoutingTable(256, List.of(
                new ShardRange(ENDPOINT_A, 0, 127),
                new ShardRange(ENDPOINT_B, 128, 255)));

            int slot = table.slotFor(Long.MIN_VALUE);

            assertTrue(slot >= 0 && slot < 256, "음수 accountId도 [0, slotCount) 범위 안의 슬롯이어야 한다");
        }
    }
}
