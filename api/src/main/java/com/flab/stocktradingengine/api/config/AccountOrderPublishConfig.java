package com.flab.stocktradingengine.api.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

import com.flab.stocktradingengine.aeron.AccountDestinationResolver;
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
@Slf4j
@Configuration
public class AccountOrderPublishConfig {

    @Bean(destroyMethod = "close")
    public MediaDriver accountOrderMediaDriver() {
        String aeronDirectoryName = CommonContext.generateRandomDirName();
        // 이 드라이버는 통신용 임시 디렉터리(aeron-*)만 갖는다(Archive 없음) — 프로세스가 죽으면
        // 쓸모없는데 기본값이 안 지워 무한히 쌓인다(I10).
        return MediaDriver.launchEmbedded(new MediaDriver.Context().aeronDirectoryName(aeronDirectoryName)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true));
    }

    @Bean(destroyMethod = "close")
    public Aeron accountOrderAeron(MediaDriver accountOrderMediaDriver) {
        return Aeron.connect(new Aeron.Context().aeronDirectoryName(accountOrderMediaDriver.aeronDirectoryName()));
    }

    /**
     * 정적 모드(U2) — {@code shard-routing.shards}가 가리키는 목적지 endpoint마다 하나씩 연다.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "account-shard.coordination", name = "enabled", havingValue = "false", matchIfMissing = true)
    public AccountOrderPublications accountOrderPublications(Aeron accountOrderAeron, ShardRoutingTable shardRoutingTable) {
        Map<String, Publication> byEndpoint = new LinkedHashMap<>();
        for (String endpoint : shardRoutingTable.distinctEndpoints()) {
            byEndpoint.put(endpoint, accountOrderAeron.addPublication(endpoint, AeronStreamIds.ACCOUNT_INTAKE));
        }
        return new AccountOrderPublications(byEndpoint);
    }

    /**
     * 동적 모드(계좌 샤딩 U5) — 담당 배정은 Kafka가 정하지만(U4), "존재하는 워커 주소가 무엇인가"는
     * 여전히 사람이 {@code shard-routing.endpoints}에 적는다(멤버 목록, 슬롯 범위 없음). 그 풀
     * 전체로 Publication을 미리 다 열어 둔다 — account-shard-map은 그 풀 중 누가 어느 슬롯을
     * 맡았는지만 알려주고({@link com.flab.stocktradingengine.aeron.AssignmentDestinationResolver}),
     * 어떤 endpoint로 Publication을 열지는 안 바꾼다.
     *
     * <p>풀에 있는데 아직 안 뜬 워커의 Publication은 연결 전(NOT_CONNECTED) 상태로 남는다 — 정상
     * 상황이라 에러로 다루지 않는다(연결 여부를 여기서 검사하지 않는다). 실제로 그 endpoint로
     * 보내려 할 때 offer가 실패하면 {@code AeronAccountOrderSender}의 재시도→503 경로가 처리한다.</p>
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "account-shard.coordination", name = "enabled", havingValue = "true")
    public AccountOrderPublications dynamicAccountOrderPublications(Aeron accountOrderAeron, ShardRoutingProperties shardRoutingProperties) {
        if (!shardRoutingProperties.shards().isEmpty()) {
            log.warn("[api] 조정 모드(account-shard.coordination.enabled=true)에서는 shard-routing.shards를 안 씁니다"
                + "(무시됨) — shard-routing.endpoints만 워커 풀로 씁니다: shards={}", shardRoutingProperties.shards());
        }
        Map<String, Publication> byEndpoint = new LinkedHashMap<>();
        for (String endpoint : shardRoutingProperties.endpoints()) {
            byEndpoint.put(endpoint, accountOrderAeron.addPublication(endpoint, AeronStreamIds.ACCOUNT_INTAKE));
        }
        log.info("[api] 워커 풀(shard-routing.endpoints) {}건으로 Publication 미리 연결: {}",
            byEndpoint.size(), shardRoutingProperties.endpoints());
        return new AccountOrderPublications(byEndpoint);
    }

    @Bean
    public AeronAccountOrderSender aeronAccountOrderSender(
            AccountDestinationResolver accountDestinationResolver, AccountOrderPublications accountOrderPublications) {
        return new AeronAccountOrderSender(accountDestinationResolver, accountOrderPublications.byEndpoint());
    }
}
