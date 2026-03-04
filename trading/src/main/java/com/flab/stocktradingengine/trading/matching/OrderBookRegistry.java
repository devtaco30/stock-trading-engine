package com.flab.stocktradingengine.trading.matching;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

/**
 * 전체 종목 호가창 및 최근 체결가(LTP) 저장소.
 *
 * <p><b>패턴: Registry Pattern</b><br>
 * 종목코드를 키로 {@link OrderBook} 을 중앙에서 관리한다.
 * 서버 재시작 시 {@code OrderBookInitializer} 가 DB 의 PENDING 주문을 로드해 채운다.</p>
 *
 * <p><b>ConcurrentHashMap 선택 이유</b><br>
 * 서로 다른 종목의 호가창 등록·조회는 동시에 일어날 수 있다.
 * {@code computeIfAbsent} 가 원자적으로 실행되므로 동일 종목에 OrderBook 이
 * 두 개 생성되는 일이 없다.
 * 각 {@link OrderBook} 내부 접근은 Kafka 파티션(종목코드 키)이 단일 컨슈머를 보장한다.</p>
 *
 * <p><b>LTP (Last Traded Price)</b><br>
 * 매칭 컨슈머(Kafka listener 스레드)가 쓰고, API 서버(HTTP 스레드)가 읽는다.
 * {@code ConcurrentHashMap} 으로 put/get 의 원자성을 보장하며,
 * {@link BigDecimal} 이 불변이므로 부분 읽기 문제가 없다.</p>
 */
@Component
public class OrderBookRegistry {

    private final ConcurrentHashMap<String, OrderBook> books = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BigDecimal> lastTradedPrices = new ConcurrentHashMap<>();

    /** 종목 호가창 반환. 없으면 새로 생성(원자적). */
    public OrderBook getOrCreate(String stockCode) {
        return books.computeIfAbsent(stockCode, k -> new OrderBook());
    }

    /** 종목 호가창 반환. 없으면 null. 취소 처리 등 조회 전용에 사용. */
    public OrderBook get(String stockCode) {
        return books.get(stockCode);
    }

    /** 체결 후 최근 체결가 갱신. 매칭 컨슈머(Single Writer per partition)만 호출. */
    public void updateLastTradedPrice(String stockCode, BigDecimal price) {
        lastTradedPrices.put(stockCode, price);
    }

    /**
     * 최근 체결가 반환. 체결이 없었던 종목은 empty.
     * 호출자는 {@code QuoteService.previousClose} 를 fallback 으로 사용해야 한다.
     */
    public Optional<BigDecimal> getLastTradedPrice(String stockCode) {
        return Optional.ofNullable(lastTradedPrices.get(stockCode));
    }

    /**
     * 파티션 반환(리밸런싱) 시 해당 종목 호가창과 LTP 제거.
     * 다른 인스턴스로 파티션이 이동하므로 이 인스턴스의 인메모리 상태를 비운다.
     */
    public void removeBook(String stockCode) {
        books.remove(stockCode);
        lastTradedPrices.remove(stockCode);
    }
}
