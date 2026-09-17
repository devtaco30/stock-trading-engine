package com.flab.stocktradingengine.account.worker.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.IntPredicate;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.flab.stocktradingengine.aeron.ShardRoutingTable;
import com.flab.stocktradingengine.account.disruptor.domain.AccountResultListener;
import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.io.AccountSnapshotSink;
import com.flab.stocktradingengine.account.disruptor.io.MatchingOrderSender;
import com.flab.stocktradingengine.account.disruptor.journal.AccountJournal;
import com.flab.stocktradingengine.account.worker.lifecycle.AccountEngineLifecycle;
import com.flab.stocktradingengine.account.worker.listener.CompositeAccountResultListener;
import com.flab.stocktradingengine.account.worker.recovery.StoredAccountSnapshot;
import com.flab.stocktradingengine.codec.AccountEventType;
import com.flab.stocktradingengine.codec.AccountJournalEntry;
import com.flab.stocktradingengine.codec.FilledTrade;
import com.flab.stocktradingengine.support.SnowflakeNodeIdResolver;
import com.flab.stocktradingengine.time.LatencyHistogram;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@EnableConfigurationProperties(AccountWorkerProperties.class)
public class AccountEngineConfig {

    private static final int BUFFER_SIZE = 1024;

    /**
     * {@link AccountResultListener} 구현체(로깅·정산 요청 발행 등)를 전부 묶어 하나의 리스너로 만든다.
     * {@link #accountEngine}은 리스너를 하나만 받으므로, 이 빈을 {@link Primary}로 두어 그 자리에 주입되게 한다.
     */
    @Bean
    @Primary
    public CompositeAccountResultListener compositeAccountResultListener(List<AccountResultListener> delegates) {
        return new CompositeAccountResultListener(delegates);
    }

    /**
     * 설정된 계좌들을 시드한 {@link AccountEngine}을 만든다. 아직 start() 는 안 부른다 — 생명주기 빈이 담당.
     *
     * <p>{@link ProducerType#MULTI}로 만든다 — 이 앱은 발행자가 하나가 아니다.
     * {@code AccountFillReceiver}(체결 Aeron 수신 스레드, ADR-032 U3)가 체결 반영을 발행하고,
     * 주문 접수 경로(Aeron 수신 스레드)도 같은 링버퍼에 검증·예약을 발행한다. SINGLE로 두면
     * 두 스레드가 동시에 발행할 때 링버퍼 시퀀스가 깨진다.</p>
     *
     * <p>orderId는 더 이상 Snowflake(벽시계)가 아니라 엔진 내부 결정론적 발급기(2b-0)가 낸다 —
     * 여기서는 nodeId만 넘긴다. matching-worker {@code SnowflakeConfig}와 같은 프로퍼티
     * ({@code snowflake.node-id})를 그대로 재사용한다(같은 노드 구분 관례).</p>
     *
     * <p>저널은 기본(인메모리) 대신 {@link AccountJournalArchiveConfig}가 만든 Aeron Archive durable
     * 구현({@code AeronArchiveAccountJournal})을 명시적으로 넘긴다(2b-1b) — 프로세스가 죽어도
     * 저널이 디스크에 남아야 2b-2 리플레이가 성립한다.</p>
     *
     * <p>시드 직후, start() 전에 스냅샷이 있으면(2d-2b) 그걸로 계좌·발급기 카운터를 먼저 덮어쓴다
     * ({@code restore} — 계좌는 seed로만 생기므로 스냅샷의 계좌 집합은 항상 seed의 계좌 집합과
     * 같아 그대로 덮어써도 안전하다). 그다음 {@link AccountJournalArchiveConfig#accountJournalRecoveredEntries}
     * (스냅샷이 있으면 그 이후분만, 없으면 전부, 2b-2b/2d-2b)를 재적용하고, 마지막으로
     * {@link AccountFillIntakeConfig#accountFillReplayedEntries}(매칭이 냈지만 이 프로세스가
     * 못 받은 체결, ADR-032 U4b)를 BUY_FILL·SELL_FILL 저널 엔트리 쌍으로 바꿔 같은 recover 경로로
     * 재적용한다 — tradeId 멱등(계좌별)이 이미 저널로 반영된 체결과의 중복을 흡수하므로, 저널
     * recover와 fill recover를 두 번 나눠 불러도 안전하다. 재시작 전 상태·dedup·orderId 발급기를
     * 되살린 뒤에야 라이브 트래픽을 받는다.</p>
     *
     * <p>{@link AccountSnapshotSink}(1-3, 실제 구현은 {@code AccountSnapshotWriter})를 러닝 중
     * 스냅샷 싱크로 넘긴다 — 인터페이스로 받아, Archive 없이 가볍게 띄우는 테스트가 {@link
     * AccountJournal}처럼 간단한 대체 빈을 넣을 수 있게 한다. 스냅샷이 있었으면
     * {@code engine.seedFillPositions}으로 소비자의 체결 수신 위치(발행자별 맵)를 그 스냅샷 값으로 시드한다 —
     * 안 하면 복구 직후~첫 라이브 체결 사이에 러닝 중 스냅샷이 찍힐 때 위치가 0으로 저장돼, 다음
     * 복구가 체결 스트림을 처음부터 다시 replay한다(39 리뷰 지적).</p>
     */
    @Bean
    public AccountEngine accountEngine(AccountWorkerProperties properties, @Value("${snowflake.node-id:}") String nodeIdConfig,
                                       MatchingOrderSender matchingOrderSender, AccountResultListener listener, AccountJournal journal,
                                       AccountSnapshotSink accountSnapshotSink, LatencyHistogram accountLatencyHistogram,
                                       ShardRoutingTable shardRoutingTable, IntPredicate ownedSlots,
                                       Optional<StoredAccountSnapshot> accountLoadedSnapshot, List<AccountJournalEntry> accountJournalRecoveredEntries,
                                       List<FilledTrade> accountFillReplayedEntries) {
        long nodeId = SnowflakeNodeIdResolver.resolve(nodeIdConfig);
        AccountEngine engine = new AccountEngine(
            BUFFER_SIZE, new BlockingWaitStrategy(), ProducerType.MULTI, nodeId, matchingOrderSender, listener, journal,
            accountSnapshotSink, accountLatencyHistogram, shardRoutingTable, ownedSlots);
        // (A안, 계좌 샤딩 U4) 담당 여부로 걸러 seed 하지 않는다 — Kafka 컨슈머 그룹 배정은 비동기라
        // 이 시점엔 아직 배정을 못 받았을 수 있다(빈 집합). 걸러서 seed 하면 그 계좌는 seed()가
        // engine.start() 전에만 되므로 배정이 나중에 와도 영원히 못 실린다. 그래서 seed-accounts를
        // 전부 싣고, 실제 소유 판정은 이벤트를 처리할 때마다 독립적으로 검증하는 런타임 isOwned()
        // (U3, AccountEventHandler.rejectIfNotOwned)가 유일한 방어선으로 맡는다. 담당 아닌 계좌가
        // 메모리에 같이 있는 대가는 이 프로젝트 규모에선 무해하다(정적 모드에서도 마찬가지).
        int ownedNow = 0;
        for (AccountWorkerProperties.SeedAccount seed : properties.seedAccounts()) {
            if (engine.owns(seed.accountId())) {
                ownedNow++;
            }
            if (seed.holdings() == null || seed.holdings().isEmpty()) {
                engine.seed(seed.accountId(), seed.balance(), seed.marginRate());
            } else {
                engine.seed(seed.accountId(), seed.balance(), seed.marginRate(), seed.holdings());
            }
        }
        // 이 시점 기준 담당 수만 안내용으로 남긴다(필터링에는 안 쓴다) — 0건이면 아직 배정 전이거나
        // 설정이 어긋난 것이라 원인 추적의 첫 단서가 된다.
        log.info("[계좌] 시드 대상 {}건 전부 시드, 이 시점 기준 담당 {}건(나머지는 배정 전이거나 남의 슬롯 — 런타임에 NOT_OWNED로 거부됨)",
            properties.seedAccounts().size(), ownedNow);
        accountLoadedSnapshot.ifPresent(stored -> {
            engine.restore(stored.snapshot());
            engine.seedFillPositions(stored.fillConsumedPosition());
        });
        engine.recover(accountJournalRecoveredEntries);
        engine.recover(toFillJournalEntries(accountFillReplayedEntries));
        return engine;
    }

    /**
     * {@link FilledTrade} 하나를 BUY_FILL·SELL_FILL {@link AccountJournalEntry} 쌍으로 바꾼다 —
     * {@code AccountFillReceiver.onFragment}(라이브 경로, U3)가 같은 체결로 publishBuyFill·
     * publishSellFill 둘 다 부르는 것과 같은 매핑이다. SELL_FILL은 price를 안 쓰므로(엔진
     * {@code AccountEvent#setSellFill} 참고) null로 둔다. requestId는 체결에 없는 필드라 null.
     */
    private List<AccountJournalEntry> toFillJournalEntries(List<FilledTrade> trades) {
        List<AccountJournalEntry> entries = new ArrayList<>();
        for (FilledTrade trade : trades) {
            entries.add(new AccountJournalEntry(AccountEventType.BUY_FILL, trade.buyOrderId(), trade.buyAccountId(),
                trade.stockCode(), trade.matchPrice(), trade.filledQuantity(), null, trade.tradeId()));
            entries.add(new AccountJournalEntry(AccountEventType.SELL_FILL, trade.sellOrderId(), trade.sellAccountId(),
                trade.stockCode(), null, trade.filledQuantity(), null, trade.tradeId()));
        }
        return entries;
    }

    @Bean
    public SmartLifecycle accountEngineLifecycle(AccountEngine engine) {
        return new AccountEngineLifecycle(engine);
    }
}
