package com.flab.stocktradingengine.trading.matching;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import com.flab.stocktradingengine.trading.entity.OrderSide;

/**
 * 단일 종목 호가창 (Order Book).
 *
 * <h3>자료구조 선택 이유</h3>
 * <ul>
 *   <li><b>TreeMap&lt;Price, Deque&lt;OrderEntry&gt;&gt;</b>:
 *       가격 기준으로 항상 정렬된 상태를 유지한다.
 *       {@code firstEntry()} 로 최우선 호가를 O(log n) 에 조회한다.</li>
 *   <li><b>bids 역순(reverseOrder)</b>: 높은 가격이 앞 → 최고 매수가를 O(1) 로 꺼냄.</li>
 *   <li><b>asks 오름차순(natural order)</b>: 낮은 가격이 앞 → 최저 매도가를 O(1) 로 꺼냄.</li>
 *   <li><b>Deque&lt;OrderEntry&gt;</b>: 동일 가격 내 FIFO (시간 우선).
 *       {@code addLast/pollFirst} 가 O(1).</li>
 *   <li><b>orderIndex(HashMap)</b>: orderId → OrderEntry 역참조.
 *       취소 요청이 들어올 때 O(1) 로 찾아 마킹.</li>
 * </ul>
 *
 * <h3>스레드 안전성</h3>
 * <p>이 객체는 Kafka 파티션(종목코드 키)이 보장하는 단일 컨슈머 스레드만 접근한다.
 * 내부 동기화가 전혀 없으며, Single Writer 는 인프라(Kafka 파티션) 레벨에서 보장된다.</p>
 *
 * <h3>Lazy Removal 전략</h3>
 * <p>취소된 주문을 Deque 중간에서 즉시 제거하면 O(n) 비용이 든다.
 * {@code cancelOrder()} 는 {@code orderIndex} 로 O(1) 에 OrderEntry 를 찾아 마킹만 하고,
 * Deque 에서는 꺼내지 않는다. 매칭 직전 {@code drainInvalid()} 가 앞에서부터
 * 무효 엔트리를 제거하므로 평균 O(1) 로 정리된다.</p>
 *
 * <h3>왜 LinkedList + Node 직접 참조를 쓰지 않는가</h3>
 * <p>Node 를 직접 참조하면 O(1) 중간 삭제가 가능하다.
 * 그러나 LinkedList 는 메모리가 비연속적이라 캐시 미스가 증가하고,
 * Node 관리 코드가 복잡해진다. 취소 주문이 Deque 앞에 오래 쌓이는 경우는 드물어
 * ArrayDeque(캐시 친화적) + Lazy Removal 조합으로 충분하다.</p>
 */
public class OrderBook {

    // 매수 호가: 높은 가격 → 낮은 가격 (내림차순)
    private final TreeMap<BigDecimal, Deque<OrderEntry>> bids =
        new TreeMap<>(Comparator.reverseOrder()); // 내림차순

    // 매도 호가: 낮은 가격 → 높은 가격 (오름차순)
    private final TreeMap<BigDecimal, Deque<OrderEntry>> asks =
        new TreeMap<>();

    // orderId → OrderEntry 직접 조회. TreeMap 전체 탐색 없이 O(1) 취소·멱등성 체크 가능.
    private final Map<Long, OrderEntry> orderIndex = new HashMap<>();

    static final int FILLED_ORDER_RETENTION_LIMIT = 5_000;
    static final Duration FILLED_ORDER_RETENTION_TTL = Duration.ofMinutes(5);

    // 전량 체결 완료된 주문 ID → 체결 시각. LRU bounded + TTL lazy 체크로 메모리 상한 보장.
    private final Map<Long, Instant> filledOrderTimestamps;

    public OrderBook() {
        this.filledOrderTimestamps = new LinkedHashMap<>(FILLED_ORDER_RETENTION_LIMIT, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, Instant> eldest) {
                return size() > FILLED_ORDER_RETENTION_LIMIT;
            }
        };
    }

    /** 테스트 전용 생성자. retentionLimit 을 작게 지정해 LRU 퇴출 동작을 검증할 때 사용. */
    OrderBook(int retentionLimit) {
        this.filledOrderTimestamps = new LinkedHashMap<>(retentionLimit, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, Instant> eldest) {
                return size() > retentionLimit;
            }
        };
    }

    /**
     * 주문을 호가창에 추가.
     * BUY → bids, SELL → asks 에 삽입. 동일 가격이면 큐 뒤에 추가(시간 우선 FIFO).
     */
    public void addOrder(OrderEntry entry) {
        orderIndex.put(entry.getOrderId(), entry);
        TreeMap<BigDecimal, Deque<OrderEntry>> book =
            entry.getSide() == OrderSide.BUY ? bids : asks;
        book.computeIfAbsent(entry.getPrice(), k -> new ArrayDeque<>())
            .addLast(entry);
    }

    /**
     * 해당 주문이 호가창에 등록돼 있는지 확인. 멱등성 체크용.
     * at-least-once 환경에서 같은 OrderPlacedEvent 가 중복 수신될 때 재등록을 방지한다.
     */
    public boolean containsOrder(Long orderId) {
        if (orderIndex.containsKey(orderId)) return true;
        Instant filledAt = filledOrderTimestamps.get(orderId);
        if (filledAt == null) return false;
        if (filledAt.isBefore(Instant.now().minus(FILLED_ORDER_RETENTION_TTL))) {
            filledOrderTimestamps.remove(orderId);
            return false;
        }
        return true;
    }

    /**
     * 주문 취소. orderIndex 에서 제거 후 cancelled 마킹.
     *
     * <p>큐에서 즉시 제거하지 않는 이유: Deque 중간 삭제는 O(n).
     * 마킹 후 {@link #match()} 호출 시 {@code drainInvalid()} 에서 O(1) 로 제거된다.</p>
     *
     * @return 취소 성공 여부 (이미 체결·취소된 주문이면 false)
     */
    public boolean cancelOrder(Long orderId) {
        OrderEntry entry = orderIndex.remove(orderId);
        if (entry == null) return false;
        entry.cancel();
        return true;
    }

    /**
     * 체결 가능한 쌍을 찾아 1회 매칭 시도.
     *
     * <p><b>매칭 조건</b>: 최우선 매수가 ≥ 최우선 매도가</p>
     * <p><b>체결가</b>: 매도 호가(ask) 기준. 일반적으로 먼저 등록된 Passive 주문 기준을 사용하며,
     * 이 구현에서는 단순화를 위해 ask 가격을 사용한다.</p>
     * <p><b>부분 체결</b>: 매수·매도 수량 중 작은 쪽만큼 체결.
     * 전량 체결된 주문은 인덱스와 큐에서 즉시 제거.</p>
     *
     * @return 체결이 발생하면 {@link FillResult}, 체결 불가면 empty
     */
    public Optional<FillResult> match() {
        
        // Lazy Removal: 매칭 전 각 호가창 앞에 쌓인 무효 엔트리를 제거
        drainInvalid(bids);
        drainInvalid(asks);

        if (bids.isEmpty() || asks.isEmpty()) return Optional.empty();

        Map.Entry<BigDecimal, Deque<OrderEntry>> bestBidLevel = bids.firstEntry();
        Map.Entry<BigDecimal, Deque<OrderEntry>> bestAskLevel = asks.firstEntry();

        // 매수 최고가 < 매도 최저가 → 체결 불가
        if (bestBidLevel.getKey().compareTo(bestAskLevel.getKey()) < 0) {
            return Optional.empty();
        }

        OrderEntry buyOrder  = bestBidLevel.getValue().peekFirst();
        OrderEntry sellOrder = bestAskLevel.getValue().peekFirst();

        int fillQty = Math.min(buyOrder.getRemainingQuantity(), sellOrder.getRemainingQuantity());
        BigDecimal matchPrice = bestAskLevel.getKey(); // 매도 호가 기준 체결가

        // 인메모리 상태 업데이트 (DB 반영은 FillResult 를 통해 비동기 처리)
        buyOrder.addFilled(fillQty);
        sellOrder.addFilled(fillQty);

        // 전량 체결된 주문은 인덱스 + 큐에서 즉시 제거
        removeIfFilled(bestBidLevel, buyOrder, bids);
        removeIfFilled(bestAskLevel, sellOrder, asks);

        return Optional.of(new FillResult(
            buyOrder.getOrderId(),  buyOrder.getAccountId(),
            sellOrder.getOrderId(), sellOrder.getAccountId(),
            fillQty, matchPrice
        ));
    }

    /**
     * 매수 호가 레벨 목록. 가격 내림차순, 최대 maxLevels 개.
     * 각 레벨은 (price → 해당 가격의 유효 잔량 합계).
     */
    public List<Map.Entry<BigDecimal, Integer>> getBidLevels(int maxLevels) {
        return priceLevels(bids, maxLevels);
    }

    /**
     * 매도 호가 레벨 목록. 가격 오름차순, 최대 maxLevels 개.
     */
    public List<Map.Entry<BigDecimal, Integer>> getAskLevels(int maxLevels) {
        return priceLevels(asks, maxLevels);
    }

    private static List<Map.Entry<BigDecimal, Integer>> priceLevels(
            TreeMap<BigDecimal, Deque<OrderEntry>> book, int maxLevels) {
        return book.entrySet().stream()
            .map(e -> {
                int qty = e.getValue().stream()
                    .filter(o -> !o.isCancelled())
                    .mapToInt(OrderEntry::getRemainingQuantity)
                    .sum();
                return Map.entry(e.getKey(), qty);
            })
            .filter(e -> e.getValue() > 0)
            .limit(maxLevels)
            .toList();
    }

    // ── private helpers ──────────────────────────────────────────────────────

    /**
     * 큐 앞에서 취소·전량체결된 엔트리를 제거 (Lazy Removal).
     * while 루프로 앞에서부터 유효 엔트리가 나올 때까지 제거한다.
     * 중간 제거 O(n) 없이 평균 O(1) 에 무효 엔트리를 정리한다.
     */
    private void drainInvalid(TreeMap<BigDecimal, Deque<OrderEntry>> book) {
        while (!book.isEmpty()) {
            Deque<OrderEntry> front = book.firstEntry().getValue();
            while (!front.isEmpty()) {
                OrderEntry e = front.peekFirst();
                if (e.isCancelled() || e.getRemainingQuantity() <= 0) {
                    front.pollFirst();
                } else {
                    break;
                }
            }
            if (front.isEmpty()) {
                book.pollFirstEntry(); // 해당 가격 레벨 전체 제거
            } else {
                break;
            }
        }
    }

    /** 전량 체결된 주문을 인덱스와 큐에서 제거. filledOrderIds 에 기록해 멱등성 체크를 유지. */
    private void removeIfFilled(Map.Entry<BigDecimal, Deque<OrderEntry>> level,
                                 OrderEntry entry,
                                 TreeMap<BigDecimal, Deque<OrderEntry>> book) {
        if (entry.isFullyFilled()) {
            level.getValue().pollFirst();
            orderIndex.remove(entry.getOrderId());
            filledOrderTimestamps.put(entry.getOrderId(), Instant.now());
            if (level.getValue().isEmpty()) {
                book.pollFirstEntry();
            }
        }
    }
}
