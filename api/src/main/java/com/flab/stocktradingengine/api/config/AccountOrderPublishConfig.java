package com.flab.stocktradingengine.api.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.aeron.ShardRoutingTable;
import com.flab.stocktradingengine.api.messaging.AeronAccountOrderSender;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;

/**
 * fork5, U1b — v2 게이트웨이가 계좌 인테이크로 주문을 발신할 Aeron 배선. 발신만 하는 클라이언트라
 * Archive(durable 녹화)는 안 둔다 — account-worker/matching-worker의 {@code *IntakeConfig}와
 * 달리 {@link MediaDriver}만 띄운다.
 *
 * <h3>계좌번호로 목적지를 고른다 (I8 U1)</h3>
 * <p>{@link ShardRoutingConfig}가 만든 {@link ShardRoutingTable}이 가리키는 목적지 endpoint마다
 * {@link Publication}을 하나씩 열어({@link AccountOrderPublications}) {@link AeronAccountOrderSender}에
 * 넘긴다 — {@code MatchingFillPublishConfig}와 같은 구조다. {@code shard-routing.shards}가 없으면
 * {@link ShardRoutingConfig}가 슬롯 1개짜리 표로 폴백해, 지금처럼 {@code transport.account-intake.channel}
 * 하나로만 나가는 예전 동작(fork1 unicast)을 그대로 유지한다.</p>
 *
 * <h3>스트림 ID (fork1, LLD §3-1·§3-2)</h3>
 * <p>스트림 ID는 {@link AeronStreamIds#ACCOUNT_INTAKE}로 account-worker
 * {@code AccountOrderIntakeConfig}와 core에서 공유한다.</p>
 */
@Configuration
public class AccountOrderPublishConfig {

    @Bean(destroyMethod = "close")
    public MediaDriver accountOrderMediaDriver() {
        String aeronDirectoryName = CommonContext.generateRandomDirName();
        return MediaDriver.launchEmbedded(new MediaDriver.Context().aeronDirectoryName(aeronDirectoryName));
    }

    @Bean(destroyMethod = "close")
    public Aeron accountOrderAeron(MediaDriver accountOrderMediaDriver) {
        return Aeron.connect(new Aeron.Context().aeronDirectoryName(accountOrderMediaDriver.aeronDirectoryName()));
    }

    @Bean(destroyMethod = "close")
    public AccountOrderPublications accountOrderPublications(Aeron accountOrderAeron, ShardRoutingTable shardRoutingTable) {
        Map<String, Publication> byEndpoint = new LinkedHashMap<>();
        for (String endpoint : shardRoutingTable.distinctEndpoints()) {
            byEndpoint.put(endpoint, accountOrderAeron.addPublication(endpoint, AeronStreamIds.ACCOUNT_INTAKE));
        }
        return new AccountOrderPublications(byEndpoint);
    }

    @Bean
    public AeronAccountOrderSender aeronAccountOrderSender(
            ShardRoutingTable shardRoutingTable, AccountOrderPublications accountOrderPublications) {
        return new AeronAccountOrderSender(shardRoutingTable, accountOrderPublications.byEndpoint());
    }
}
