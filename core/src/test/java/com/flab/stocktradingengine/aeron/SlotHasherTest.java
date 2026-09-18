package com.flab.stocktradingengine.aeron;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SlotHasherTest {

    @Test
    @DisplayName("같은 계좌번호는 언제나 같은 칸으로 간다")
    void 같은_계좌번호는_같은_칸으로_간다() {
        SlotHasher hasher = new SlotHasher(256);

        assertEquals(hasher.slotFor(90001L), hasher.slotFor(90001L));
        assertNotEquals(hasher.slotFor(90001L), hasher.slotFor(90005L));
    }

    @Test
    @DisplayName("칸 번호는 언제나 0 이상 칸 개수 미만이다")
    void 칸_번호는_범위_안이다() {
        SlotHasher hasher = new SlotHasher(256);

        for (long accountId = 1L; accountId <= 5000L; accountId++) {
            int slot = hasher.slotFor(accountId);
            assertTrue(slot >= 0 && slot < 256, "칸 번호가 범위를 벗어났다: accountId=" + accountId + " slot=" + slot);
        }
    }

    @Test
    @DisplayName("칸 개수가 다르면 같은 계좌도 다른 칸으로 간다 — 그래서 모든 앱이 같은 개수를 써야 한다")
    void 칸_개수가_다르면_결과가_달라진다() {
        assertEquals(0, new SlotHasher(1).slotFor(90005L));
        assertNotEquals(0, new SlotHasher(256).slotFor(90005L));
    }

    @Test
    @DisplayName("정적 모드의 라우팅 표와 같은 칸을 계산한다")
    void 라우팅_표와_같은_칸을_계산한다() {
        ShardRoutingTable table = new ShardRoutingTable(256,
            List.of(new ShardRoutingTable.ShardRange("aeron:ipc", 0, 255)));
        SlotHasher hasher = new SlotHasher(256);

        for (long accountId = 90000L; accountId <= 90100L; accountId++) {
            assertEquals(table.slotFor(accountId), hasher.slotFor(accountId));
        }
    }

    @Test
    @DisplayName("칸 개수가 1보다 작으면 만들 수 없다")
    void 칸_개수가_1보다_작으면_예외() {
        assertThrows(IllegalArgumentException.class, () -> new SlotHasher(0));
    }
}
