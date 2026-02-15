package com.flab.stocktradingengine.market.entity;

import java.math.BigDecimal;
import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 시세 (현재가 등)
 * <p>종목별 최신 시세. 시세 업데이트 배치가 주기적으로 갱신.</p>
 */
@NoArgsConstructor
@Getter
@Entity
@Table(name = "quotes")
public class Quote {

    @Id
    @Column(nullable = false, length = 10)
    private String stockCode;

    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "stock_code", referencedColumnName = "stock_code", insertable = false, updatable = false)
    private Stock stock;

    @Column(nullable = false, precision = 12, scale = 0)
    private BigDecimal currentPrice;

    @Column(nullable = false, precision = 12, scale = 0)
    private BigDecimal previousClose;

    @Column(precision = 8, scale = 2)
    private BigDecimal changeRate;

    @Column(precision = 12, scale = 0)
    private BigDecimal open;

    @Column(precision = 12, scale = 0)
    private BigDecimal high;

    @Column(precision = 12, scale = 0)
    private BigDecimal low;

    @Column
    private Long volume;

    @Column(nullable = false)
    private Instant updatedAt;
}
