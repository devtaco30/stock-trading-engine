package com.flab.stocktradingengine.account.worker.config;

import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.flab.stocktradingengine.account.disruptor.engine.AccountEngine;
import com.flab.stocktradingengine.account.disruptor.io.AccountFillReceiver;
import com.flab.stocktradingengine.account.worker.lifecycle.AccountFillReceiverLifecycle;
import com.flab.stocktradingengine.account.worker.recovery.AccountFillReplayer;
import com.flab.stocktradingengine.account.worker.recovery.AccountSnapshotStore;
import com.flab.stocktradingengine.account.worker.recovery.StoredAccountSnapshot;
import com.flab.stocktradingengine.aeron.AeronStreamIds;
import com.flab.stocktradingengine.codec.FilledTrade;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;

/**
 * ADR-032, U3 — 매칭이 Aeron으로 발행하는 체결을 받는 인테이크 경로 배선. {@link AccountOrderIntakeConfig}가
 * 만든 account-worker 자체 Aeron(MediaDriver)을 그대로 써서 Subscription만 새로 연다(그 결과
 * 클래스와 같은 결). Kafka {@code AccountFillConsumer}를 대체한다 — 정산 컨슈머(account-settlements)는
 * 그대로 Kafka에 남는다({@link AccountKafkaErrorHandlerConfig} 참고).
 *
 * <p>스트림 ID는 {@link com.flab.stocktradingengine.aeron.AeronStreamIds#FILL}로 matching-worker의
 * {@code MatchingFillPublishConfig}와 core에서 공유한다. 채널은 {@code transport.fill.channel}
 * 속성에서 해석된다(fork1 Unit 1) — 두 앱은 모듈 경계상 서로 의존하지 않아 값 일치를 코드가
 * 강제하진 못하므로, 같은 값을 각자의 config에 넣어야 한다.</p>
 *
 * <h3>순서 (전달 유실 방지)</h3>
 * <p>①{@link #accountFillReplayedEntries}(이전 녹화 읽기) → ②
 * {@link #accountFillRecordingSubscriptionId}(새 녹화 시작, REMOTE) → ③
 * {@link #accountFillSubscription}(라이브 구독) 순서로 만들어지도록 각자 앞 단계 빈을 파라미터로
 * 받아 의존시킨다 — {@link AccountJournalArchiveConfig} 클래스 javadoc "순서" 절과 같은 이유다
 * (새 구독자는 붙은 시점 이후 데이터만 보므로, 녹화가 라이브 구독보다 늦게 시작되면 그 사이
 * 반영된 체결이 녹화엔 없는 유실 창이 생긴다).</p>
 *
 * <h3>왜 REMOTE인가 (fork3, U2~U3 durability 이동)</h3>
 * <p>녹화 주체는 받는 쪽(계좌)이다(fork1 D 결정). 이 스트림의 발행자는 매칭(다른 프로세스·다른
 * Aeron 드라이버)이라, 같은 드라이버 안의 발행을 엿듣는 spy(LOCAL)가 아니라 일반 구독으로 기록하는
 * {@link SourceLocation#REMOTE}를 쓴다 — udp 크로스프로세스에서 정답이고, {@code aeron:ipc}(테스트·
 * shard-routing 미설정 폴백)에서도 같은 드라이버 안의 ipc 발행을 일반 구독이 그대로 받으므로
 * 동일하게 동작한다. fork3 U2가 매칭측 LOCAL 녹화를 제거하면서 생긴 durability 공백을 여기서 닫는다.</p>
 */
@Configuration
public class AccountFillIntakeConfig {

    // 패키지 가시성 — 테스트(같은 패키지)가 이 기본값을 그대로 참조해 matching-worker 체결
    // 채널과 맞춘다. 실제 채널은 transport.fill.channel 속성에서 해석된다(fork1 Unit 1). 스트림
    // ID는 core AeronStreamIds.FILL로 matching-worker와 공유한다.
    static final String DEFAULT_FILL_CHANNEL = "aeron:ipc";

    /**
     * 체결 스트림에 새 녹화를 시작한다(REMOTE, 클래스 javadoc 참고). 반환값(Archive 구독 ID)은
     * 안 쓴다 — 이 빈이 존재하는 이유는 {@link #accountFillSubscription}이 이 빈에 의존하게 만들어
     * 생성 순서를 강제하는 것뿐이다. {@code accountFillReplayedEntries}를 파라미터로 받는 이유도
     * 같다 — 이전 녹화 리플레이가 끝난 뒤에야 새 녹화를 시작해야 한다({@link AccountJournalArchiveConfig}와
     * 같은 이유).
     */
    @Bean
    public Long accountFillRecordingSubscriptionId(
            AeronArchive aeronArchive,
            List<FilledTrade> accountFillReplayedEntries,
            @Value("${transport.fill.channel:" + DEFAULT_FILL_CHANNEL + "}") String fillChannel) {
        return aeronArchive.startRecording(fillChannel, AeronStreamIds.FILL, SourceLocation.REMOTE);
    }

    @Bean(destroyMethod = "close")
    public Subscription accountFillSubscription(
            Aeron aeron,
            Long accountFillRecordingSubscriptionId,
            @Value("${transport.fill.channel:" + DEFAULT_FILL_CHANNEL + "}") String fillChannel) {
        return aeron.addSubscription(fillChannel, AeronStreamIds.FILL);
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
    public List<FilledTrade> accountFillReplayedEntries(
            AeronArchive aeronArchive,
            AccountSnapshotStore accountSnapshotStore,
            @Value("${transport.fill.channel:" + DEFAULT_FILL_CHANNEL + "}") String fillChannel) {
        Optional<StoredAccountSnapshot> accountLoadedSnapshot = accountSnapshotStore.read();
        if (accountLoadedSnapshot.isEmpty()) {
            return List.of();
        }
        AccountFillReplayer replayer = new AccountFillReplayer(aeronArchive);
        return replayer.readFrom(fillChannel, AeronStreamIds.FILL, accountLoadedSnapshot.get().fillConsumedPosition());
    }
}
