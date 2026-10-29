package com.flab.stocktradingengine.aeron;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

class AeronStreamIdsTest {

    @Test
    void 선언된_스트림_번호가_서로_겹치지_않는다() {
        List<Integer> ids = List.of(
            AeronStreamIds.ACCOUNT_INTAKE, AeronStreamIds.MATCHING_INTAKE, AeronStreamIds.FILL,
            AeronStreamIds.ORDER_RESULT, AeronStreamIds.REJECT_REPLY,
            AeronStreamIds.ORDER_BOOK_REQUEST, AeronStreamIds.ORDER_BOOK_RESPONSE);

        Set<Integer> distinct = new HashSet<>(ids);

        assertEquals(ids.size(), distinct.size());
    }

    @Test
    void 거부_회신과_호가창_요청_응답은_정해진_번호이고_기존_번호와_다르다() {
        assertEquals(6003, AeronStreamIds.REJECT_REPLY);
        assertEquals(2008, AeronStreamIds.ORDER_BOOK_REQUEST);
        assertEquals(2009, AeronStreamIds.ORDER_BOOK_RESPONSE);
        Set<Integer> existing = Set.of(4004, 2002, 6001, 4007);
        assertFalse(existing.contains(AeronStreamIds.REJECT_REPLY));
        assertFalse(existing.contains(AeronStreamIds.ORDER_BOOK_REQUEST));
        assertFalse(existing.contains(AeronStreamIds.ORDER_BOOK_RESPONSE));
    }
}
