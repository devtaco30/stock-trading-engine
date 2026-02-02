package com.flab.stocktradingengine.market.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 종목 마스터
 * <p>한국거래소(KRX) 공식 종목 코드·종목명. 종목 존재 여부·검색에 사용.</p>
 */
@NoArgsConstructor
@Getter
@Entity
@Table(name = "stocks")
public class Stock {

    @Id
    @Column(nullable = false, length = 10)
    private String stockCode;

    @Column(nullable = false, length = 100)
    private String stockName;

    @Column(nullable = false, length = 20)
    private String mrktCtg;
}
