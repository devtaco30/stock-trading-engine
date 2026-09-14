package com.flab.stocktradingengine.account.worker.config;

import java.util.List;
import java.util.Optional;

import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.io.AccountFillReceiver;
import com.flab.stocktradingengine.account.worker.lifecycle.AccountFillReceiverLifecycle;
import com.flab.stocktradingengine.account.worker.recovery.AccountFillReplayer;
import com.flab.stocktradingengine.account.worker.recovery.AccountSnapshotStore;
import com.flab.stocktradingengine.account.worker.recovery.StoredAccountSnapshot;
import com.flab.stocktradingengine.codec.FilledTrade;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;

/**
 * ADR-032, U3 — 매칭이 Aeron으로 발행하는 체결을 받는 인테이크 경로 배선. {@link AccountOrderIntakeConfig}가
 * 만든 account-worker 자체 Aeron(MediaDriver)을 그대로 써서 Subscription만 새로 연다(그 결과
 * 클래스와 같은 결). Kafka {@code AccountFillConsumer}를 대체한다 — 정산 컨슈머(account-settlements)는
 * 그대로 Kafka에 남는다({@link AccountKafkaErrorHandlerConfig} 참고).
 *
 * <p>채널·스트림은 matching-worker의 {@code MatchingFillPublishConfig.FILL_STREAM_ID}(6001)와
 * 반드시 같아야 한다 — account-worker는 모듈 경계상 matching-worker에 의존하지 않아 상수를
 * 공유하지 못하고 값만 맞춘다({@code MatchingOrderSenderConfig}와 같은 이유).</p>
 */
@Configuration
public class AccountFillIntakeConfig {

    // 패키지 가시성 — 테스트(같은 패키지)가 이 값을 그대로 참조해 matching-worker 체결 스트림과 맞춘다.
    static final String FILL_CHANNEL = "aeron:ipc";
    static final int FILL_STREAM_ID = 6001; // matching-worker MatchingFillPublishConfig.FILL_STREAM_ID와 동일해야 함

    @Bean(destroyMethod = "close")
    public Subscription accountFillSubscription(Aeron aeron) {
        return aeron.addSubscription(FILL_CHANNEL, FILL_STREAM_ID);
    }

    @Bean
    public AccountFillReceiver accountFillReceiver(Subscription accountFillSubscription, AccountEngine accountEngine) {
        return new AccountFillReceiver(accountFillSubscription, accountEngine);
    }

    /**
     * {@link AccountEngineConfig#accountEngineLifecycle}보다 늦은 phase로 둔다 —
     * {@link AccountOrderIntakeConfig#accountOrderReceiverLifecycle}과 같은 이유.
     */
    @Bean
    public SmartLifecycle accountFillReceiverLifecycle(AccountFillReceiver accountFillReceiver) {
        return new AccountFillReceiverLifecycle(accountFillReceiver);
    }

    /**
     * 재기동 시 "매칭이 냈지만 이 프로세스가 못 받은 체결"을 채운다(ADR-032, U4b). 스냅샷이
     * 가리키는 {@link StoredAccountSnapshot#fillConsumedPosition}부터 체결 스트림을 replay해
     * {@link AccountEngineConfig#accountEngine}이 {@code engine.recover(...)}로 재적용한다.
     *
     * <p>스냅샷이 없으면(첫 기동, graceful stop을 한 번도 안 겪음) 되살릴 gap 자체가 없다 —
     * fromPosition을 정할 기준이 없으므로 replay를 생략하고 빈 리스트를 돌려준다. 이후 들어오는
     * 체결은 {@link AccountFillReceiver}가 라이브로 받는다.</p>
     *
     * <p>{@code Optional<StoredAccountSnapshot>}(={@code accountLoadedSnapshot} 빈)을 파라미터로
     * 받지 않고 {@link AccountSnapshotStore}를 직접 받아 {@code read()}를 한 번 더 부른다 — Spring이
     * {@code Optional<T>} 타입 파라미터를 "빈 T 하나를 찾아 감싸기"로 처리해, 실제로 존재하는
     * {@code Optional<StoredAccountSnapshot>} 리턴 타입의 {@code accountLoadedSnapshot} 빈 자체를
     * 못 찾고 빈 값을 주는 경우가 있었다(직접 겪음 — 다른 빈 그래프 순서에선 되고 이 자리에선
     * 안 됨). 파일 읽기가 가벼워 두 번 불러도 비용이 무시할 만하다.</p>
     */
    @Bean
    public List<FilledTrade> accountFillReplayedEntries(AeronArchive aeronArchive, AccountSnapshotStore accountSnapshotStore) {
        Optional<StoredAccountSnapshot> accountLoadedSnapshot = accountSnapshotStore.read();
        if (accountLoadedSnapshot.isEmpty()) {
            return List.of();
        }
        AccountFillReplayer replayer = new AccountFillReplayer(aeronArchive);
        return replayer.readFrom(FILL_CHANNEL, FILL_STREAM_ID, accountLoadedSnapshot.get().fillConsumedPosition());
    }
}
