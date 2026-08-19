package com.flab.stocktradingengine.settlement.entity;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 처리 완료된 체결 기록 (멱등성 마커).
 * <p>at-least-once 환경에서 같은 {@code TradeFilledEvent}가 재전달돼도 중복 반영되지 않도록,
 * 이미 반영한 체결의 {@code tradeId}를 기록한다. tradeId를 자연 PK로 사용해
 * 존재 여부만으로 중복을 판별한다.</p>
 */
@NoArgsConstructor
@Getter
@Entity
@Table(name = "processed_fills")
public class ProcessedFill {

    @Id
    @Column(name = "trade_id")
    private Long tradeId;

    @Column(nullable = false)
    private Instant processedAt;

    public ProcessedFill(Long tradeId, Instant processedAt) {
        this.tradeId = tradeId;
        this.processedAt = processedAt;
    }
}
