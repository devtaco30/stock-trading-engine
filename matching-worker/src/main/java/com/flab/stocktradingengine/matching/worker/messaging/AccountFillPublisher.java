package com.flab.stocktradingengine.matching.worker.messaging;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.LinkedHashMap;
import java.util.Map;

import com.flab.stocktradingengine.aeron.AccountDestinationResolver;
import com.flab.stocktradingengine.codec.FillCodec;
import com.flab.stocktradingengine.codec.FilledTrade;
import com.flab.stocktradingengine.matching.disruptor.io.MatchListener;
import com.flab.stocktradingengine.support.SnowflakeIdGenerator;
import com.flab.stocktradingengine.trading.matching.FillResult;

import io.aeron.ExclusivePublication;

/**
 * 매칭 코어의 체결을 계좌 샤드별 Aeron 스트림으로 fan-out 발행하는 {@link MatchListener}
 * 구현체(ADR-032 U2, fork3 U2 — I4에서 발신을 {@link FillOutbox}로 옮겨 재구성). {@link
 * ShardRoutingTable}로 매수·매도 계좌가 각각 속한 샤드 endpoint를 계산해 같은 {@link FilledTrade}를
 * 그 목적지들로 보낸다(수신 모델 A — 각 샤드가 매수·매도 둘 다 시도해 자기 소유 계좌만 반영, 소유
 * 검증·반영은 계좌측(유닛 3) 몫). 두 계좌가 같은 endpoint면 큐에 한 번만 넣어 중복 발행을 막는다.
 *
 * <h3>매칭 소비자 스레드는 발신에 묶이지 않는다(I4)</h3>
 * <p>{@link #onFill}은 매칭 단일 소비자 스레드에서만 불린다. 예전에는 이 스레드가 직접 Aeron
 * {@code offer}를 성공할 때까지 재시도해, 계좌 샤드 하나가 못 받으면 그 재시도 루프에서 이 스레드가
 * 묶여 매칭 노드가 맡은 종목 전체가 멈췄다. 지금은 {@link #onFill}이 tradeId를 발급해 {@link
 * FilledTrade}를 만들고 목적지별 {@link FillOutbox}에 큐잉만 하고 돌아온다 — 실제 인코딩·Aeron
 * 발신·never-drop 재시도는 그 endpoint 전용 스레드가 한다. endpoint마다 큐·스레드가 따로라 한
 * endpoint가 막혀도 다른 endpoint의 발신은 영향받지 않는다.</p>
 *
 * <p>체결은 여전히 버리지 않는다 — {@link FillOutbox#enqueueNeverDrop}이 큐가 가득 차면 매칭
 * 소비자 스레드를 그 자리에서 대기시킨다. 큐 용량만큼만 버틴다는 뜻이라, 계좌 샤드가 영구히 안
 * 살아나는 경우까지 닫지는 않는다(대기 프로세스를 두는 I8의 몫).</p>
 *
 * <h3>목적지 조회 (계좌 샤딩 U5·U6) — 목적지를 못 찾는 경우 셋을 서로 다르게 다룬다</h3>
 * <p>{@link com.flab.stocktradingengine.aeron.ShardRoutingTable}을 직접 참조하던 것을
 * {@link AccountDestinationResolver}로 갈아탔다(api의 {@code AeronAccountOrderSender}·U2와 같은
 * 이유). 그런데 이 인터페이스 하나로 목적지를 못 찾는 상황이 성격이 다른 셋으로 갈린다 — 같은
 * 예외로 뭉뚱그리면 안 된다(U6에서 이 차이를 코드로 갈랐다).</p>
 * <ol>
 *   <li><b>기동 직후, account-shard-map을 아직 못 읽었다</b> — {@code MatchingOrderReceiverLifecycle}이
 *       {@code AssignmentDestinationResolver}의 초기 읽기(그 시점까지 쌓인 기록 전부)가 끝난
 *       뒤에야 주문 인테이크 구독을 연다. 그래서 매칭이 실제로 주문을 받기 시작했을 때는 이미
 *       그 시점까지의 배정을 다 안다 — 이 경우는 애초에 안 생기게 막았다(구조 변경이 아니라
 *       기동 순서).</li>
 *   <li><b>운영 중에 그 슬롯의 주인이 없다(계좌 워커가 죽어 tombstone된 슬롯)</b> — ⚠️ <b>미해결.</b>
 *       {@link #requireEndpoint}가 {@link IllegalStateException}을 던지고, 이건
 *       {@code MatchingEventHandler.onEvent}의 catch에 걸려 그 체결이 "이벤트 폐기" 로그 한 줄과
 *       함께 조용히 사라진다. 재전송·대기열은 없다 — 큐·상한·주인이 돌아왔을 때 재발행까지
 *       얽혀 있어 이 트랙(U6) 범위 밖으로 남겨 뒀다. 계좌 워커 하나가 죽어 있는 동안 그 슬롯의
 *       체결은 유실될 수 있다.</li>
 *   <li><b>목적지는 알지만 그 endpoint가 shard-routing.endpoints 풀에 없다(설정 어긋남)</b> —
 *       {@link #enqueue}가 {@link AccountDestinationMisconfiguredException}을 던진다. 이 타입은
 *       {@code onEvent}의 catch(IllegalArgumentException·IllegalStateException만 잡음)를 피해
 *       {@code MatchingExceptionHandler}까지 올라가 매칭 소비자를 진짜로 멈춘다 — 재시도해도
 *       안 나아지는 설정 오류라 계속 돌면 같은 이유로 유실이 반복되기 때문이다.</li>
 * </ol>
 *
 * <p>{@code stockCode}·{@code tradeId}는 {@link FillResult}에 없다. stockCode는 {@link #onFill}의
 * 인자로 받고, tradeId는 체결이 일어난 순서와 어긋나지 않도록 지금처럼 이 스레드에서 체결 1건당
 * 한 번만 발급한다(발급 자리를 {@link FillOutbox}로 옮기지 않는다).</p>
 */
public class AccountFillPublisher implements MatchListener {

    private static final Logger log = System.getLogger(AccountFillPublisher.class.getName());
    private static final long CLOSE_DRAIN_TIMEOUT_MILLIS = 5000L;

    private final AccountDestinationResolver destinationResolver;
    private final SnowflakeIdGenerator snowflakeIdGenerator;
    private final Map<String, FillOutbox> outboxesByEndpoint;

    public AccountFillPublisher(
            AccountDestinationResolver destinationResolver,
            Map<String, ExclusivePublication> publicationsByEndpoint,
            SnowflakeIdGenerator snowflakeIdGenerator) {
        this.destinationResolver = destinationResolver;
        this.snowflakeIdGenerator = snowflakeIdGenerator;
        this.outboxesByEndpoint = buildOutboxes(publicationsByEndpoint);
    }

    private static Map<String, FillOutbox> buildOutboxes(Map<String, ExclusivePublication> publicationsByEndpoint) {
        FillCodec codec = new FillCodec();
        Map<String, FillOutbox> outboxes = new LinkedHashMap<>();
        for (Map.Entry<String, ExclusivePublication> entry : publicationsByEndpoint.entrySet()) {
            outboxes.put(entry.getKey(), new FillOutbox(entry.getKey(), entry.getValue(), codec));
        }
        return outboxes;
    }

    @Override
    public void onFill(String stockCode, FillResult fill) {
        long tradeId = snowflakeIdGenerator.nextId();
        FilledTrade trade = new FilledTrade(
            tradeId, stockCode, fill.buyOrderId(), fill.buyAccountId(),
            fill.sellOrderId(), fill.sellAccountId(), fill.filledQuantity(), fill.matchPrice());

        String buyEndpoint = requireEndpoint(fill.buyAccountId());
        String sellEndpoint = requireEndpoint(fill.sellAccountId());
        enqueue(buyEndpoint, trade);
        if (!sellEndpoint.equals(buyEndpoint)) {
            enqueue(sellEndpoint, trade);
        }
    }

    private String requireEndpoint(long accountId) {
        return destinationResolver.fillEndpointFor(accountId)
            .orElseThrow(() -> new IllegalStateException(
                "계좌 " + accountId + "의 체결 fan-out 목적지를 아직 찾을 수 없습니다(배정 대기 중일 수 있음)"));
    }

    private void enqueue(String endpoint, FilledTrade trade) {
        FillOutbox outbox = outboxesByEndpoint.get(endpoint);
        if (outbox == null) {
            // account-shard-map(동적 모드)이 가리키는 endpoint가 shard-routing.endpoints 풀에
            // 없다 — 설정이 실제 배포와 어긋났다는 신호다(계좌 샤딩 U5·U6). 재시도해도 안 나아지는
            // 설정 오류라 IllegalStateException이 아니라 AccountDestinationMisconfiguredException을
            // 던져 MatchingEventHandler의 "이벤트만 폐기" catch를 피하고 fail-fast로 간다.
            log.log(Level.WARNING, "[매칭] account-shard-map이 가리키는 endpoint가 shard-routing.endpoints 풀에 없습니다"
                + "(설정 확인 필요, fail-fast): endpoint=" + endpoint + " 풀=" + outboxesByEndpoint.keySet());
            throw new AccountDestinationMisconfiguredException("체결 fan-out 목적지에 대응하는 발행 스트림이 없습니다: " + endpoint);
        }
        outbox.enqueueNeverDrop(trade);
    }

    /** endpoint별 발신 스레드를 기동한다. Spring 빈 {@code initMethod}로 호출된다. */
    public void start() {
        outboxesByEndpoint.values().forEach(FillOutbox::start);
    }

    /** endpoint별 발신 스레드를 멈추고 큐에 남은 체결을 드레인한다. Spring 빈 {@code destroyMethod}로 호출된다. */
    public void close() {
        outboxesByEndpoint.values().forEach(outbox -> outbox.close(CLOSE_DRAIN_TIMEOUT_MILLIS));
    }
}
