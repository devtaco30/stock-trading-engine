package com.flab.stocktradingengine.account.worker.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import org.agrona.DirectBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.flab.stocktradingengine.account.worker.coordination.MatchingDestinationMisconfiguredException;
import com.flab.stocktradingengine.aeron.MatchingSlotTarget;
import com.flab.stocktradingengine.account.worker.coordination.MatchingTargetLookup;
import com.flab.stocktradingengine.aeron.StockSlotHasher;
import com.flab.stocktradingengine.codec.EventType;
import com.flab.stocktradingengine.codec.JournaledOrder;
import com.flab.stocktradingengine.codec.OrderCodec;
import com.flab.stocktradingengine.trading.entity.OrderSide;

import io.aeron.Publication;

/**
 * 매칭 샤딩 U9 — {@link AeronMatchingOrderSender}가 queue에서 꺼낸 주문의 종목코드로 담당 매칭 pod의
 * Publication을 고르고, 담당이 없거나 이동 중이면 종목 slot별 보관 목록에 두었다가 담당이 생기면
 * 들어온 순서대로 보내는지 확인한다. 거부 회신으로 돌아온 주문은 재전송 대기 목록(보관 목록의 앞)에
 * 들어간다. 담당 조회(resolver)는 대역으로, Publication은 Mockito로 만든다.
 */
class AeronMatchingOrderSenderRoutingTest {

    private static final String ADDRESS_ONE = "aeron:udp?endpoint=localhost:20020";
    private static final String ADDRESS_TWO = "aeron:udp?endpoint=localhost:20021";
    private static final int STOCK_SLOT_COUNT = 64;
    private static final long ACCOUNT_ID = 1L;
    private static final int AWAIT_MILLIS = 3000;

    private final StockSlotHasher stockSlotHasher = new StockSlotHasher(STOCK_SLOT_COUNT);
    private final String stockOne = "005930";
    private final String stockTwo = stockInDifferentSlot(stockOne);
    private final String stockThree = stockInDifferentSlot(stockOne, stockTwo);

    private final Map<String, MatchingSlotTarget> targetByStockCode = new ConcurrentHashMap<>();
    private final MatchingTargetLookup lookupDouble = () -> stockCode -> Optional.ofNullable(targetByStockCode.get(stockCode));

    private AeronMatchingOrderSender sender;

    /** 보낸 주문을 보낸 순서대로 기록하는 Publication 대역. 결과 값은 테스트가 바꾼다. */
    private static final class RecordingPublication {
        final Publication publication = mock(Publication.class);
        final List<JournaledOrder> sent = new CopyOnWriteArrayList<>();
        final AtomicLong offerResult = new AtomicLong(100L);
        private final OrderCodec codec = new OrderCodec();

        RecordingPublication() {
            when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenAnswer(invocation -> {
                long result = offerResult.get();
                if (result >= 0) {
                    DirectBuffer buffer = invocation.getArgument(0);
                    sent.add(codec.decode(buffer, 0));
                }
                return result;
            });
        }

        List<Long> sentOrderIds() {
            return sent.stream().map(JournaledOrder::orderId).toList();
        }
    }

    @AfterEach
    void tearDown() {
        if (sender != null) {
            sender.close();
        }
    }

    @Test
    void 종목마다_담당_주소의_Publication으로_나간다() {
        RecordingPublication addressOne = new RecordingPublication();
        RecordingPublication addressTwo = new RecordingPublication();
        targetByStockCode.put(stockOne, new MatchingSlotTarget(ADDRESS_ONE, false));
        targetByStockCode.put(stockTwo, new MatchingSlotTarget(ADDRESS_TWO, false));
        sender = newSender(addressOne, addressTwo, 100);
        sender.start();

        forward(1L, stockOne);
        forward(2L, stockTwo);
        forward(3L, stockOne);

        await(() -> addressOne.sent.size() == 2 && addressTwo.sent.size() == 1, "세 주문이 나가기");
        assertThat(addressOne.sentOrderIds()).containsExactly(1L, 3L);
        assertThat(addressTwo.sentOrderIds()).containsExactly(2L);
        assertThat(addressOne.sent.get(0).stockCode()).isEqualTo(stockOne);
        assertThat(addressTwo.sent.get(0).stockCode()).isEqualTo(stockTwo);
    }

    @Test
    void 어느_Publication이든_CLOSED면_이후_forwardPlace가_예외를_던진다() {
        RecordingPublication addressOne = new RecordingPublication();
        RecordingPublication addressTwo = new RecordingPublication();
        addressTwo.offerResult.set(Publication.CLOSED);
        targetByStockCode.put(stockOne, new MatchingSlotTarget(ADDRESS_ONE, false));
        targetByStockCode.put(stockTwo, new MatchingSlotTarget(ADDRESS_TWO, false));
        sender = newSender(addressOne, addressTwo, 100);
        sender.start();

        forward(1L, stockTwo);

        await(() -> throwsOnForward(2L, stockOne), "복구 불가 상태가 forwardPlace 예외로 전파되기");
    }

    @Test
    void close는_queue에_남은_주문을_모두_드레인한다() {
        RecordingPublication addressOne = new RecordingPublication();
        RecordingPublication addressTwo = new RecordingPublication();
        targetByStockCode.put(stockOne, new MatchingSlotTarget(ADDRESS_ONE, false));
        targetByStockCode.put(stockTwo, new MatchingSlotTarget(ADDRESS_TWO, false));
        sender = newSender(addressOne, addressTwo, 100);
        sender.start();

        for (long orderId = 1; orderId <= 1000; orderId++) {
            forward(orderId, orderId % 2 == 0 ? stockOne : stockTwo);
        }
        sender.close();

        assertThat(addressOne.sent.size() + addressTwo.sent.size()).isEqualTo(1000);
    }

    @Test
    void 한_주소의_Publication이_계속_BACK_PRESSURED이면_다른_주소로_가는_주문도_나가지_못한다() throws InterruptedException {
        RecordingPublication addressOne = new RecordingPublication();
        RecordingPublication addressTwo = new RecordingPublication();
        addressOne.offerResult.set(Publication.BACK_PRESSURED);
        targetByStockCode.put(stockOne, new MatchingSlotTarget(ADDRESS_ONE, false));
        targetByStockCode.put(stockTwo, new MatchingSlotTarget(ADDRESS_TWO, false));
        sender = newSender(addressOne, addressTwo, 100);
        sender.start();

        forward(1L, stockOne);
        forward(2L, stockTwo);

        Thread.sleep(300);
        assertThat(addressTwo.sent).as("queue가 1개라 앞 주문이 막히면 뒤 주문도 못 나간다").isEmpty();

        addressOne.offerResult.set(100L);
        await(() -> addressOne.sent.size() == 1 && addressTwo.sent.size() == 1, "막힘이 풀린 뒤 둘 다 나가기");
    }

    @Test
    void 담당이_없으면_주문을_보관하고_담당이_생기면_들어온_순서대로_보낸다() throws InterruptedException {
        RecordingPublication addressOne = new RecordingPublication();
        RecordingPublication addressTwo = new RecordingPublication();
        sender = newSender(addressOne, addressTwo, 100);
        sender.start();

        forward(1L, stockOne);
        forward(2L, stockOne);
        forward(3L, stockOne);
        await(() -> !sender.hasHoldingRoom(stockOne) || holdingCount(stockOne) == 3, "세 주문이 보관되기");
        Thread.sleep(100);
        assertThat(addressOne.sent).as("담당이 없는 동안은 보내지 않는다").isEmpty();

        targetByStockCode.put(stockOne, new MatchingSlotTarget(ADDRESS_ONE, false));

        await(() -> addressOne.sent.size() == 3, "담당이 생긴 뒤 보관된 주문이 나가기");
        assertThat(addressOne.sentOrderIds()).containsExactly(1L, 2L, 3L);
        assertThat(holdingCount(stockOne)).isZero();
    }

    @Test
    void 이동_중_표시를_본_종목_slot의_새_주문은_보관되고_거부_회신_주문은_보관_목록_앞에_들어간다() {
        RecordingPublication addressOne = new RecordingPublication();
        RecordingPublication addressTwo = new RecordingPublication();
        targetByStockCode.put(stockOne, new MatchingSlotTarget(ADDRESS_ONE, true));
        sender = newSender(addressOne, addressTwo, 100);
        sender.start();

        forward(10L, stockOne);
        forward(11L, stockOne);
        await(() -> holdingCount(stockOne) == 2, "새 주문 둘이 보관되기");
        sender.acceptRejectedOrder(placeOrder(1L, stockOne));
        sender.acceptRejectedOrder(placeOrder(2L, stockOne));
        await(() -> holdingCount(stockOne) == 4, "거부 회신 주문 둘이 보관 목록에 들어가기");
        assertThat(addressOne.sent).isEmpty();

        targetByStockCode.put(stockOne, new MatchingSlotTarget(ADDRESS_TWO, false));

        await(() -> addressTwo.sent.size() == 4, "맵이 바뀐 뒤 네 주문이 나가기");
        assertThat(addressTwo.sentOrderIds())
            .as("돌아온 주문(1, 2)이 먼저, 그다음 새 주문(10, 11)")
            .containsExactly(1L, 2L, 10L, 11L);
    }

    @Test
    void 담당이_아니라는_거부_회신을_받은_횟수를_센다() {
        RecordingPublication addressOne = new RecordingPublication();
        RecordingPublication addressTwo = new RecordingPublication();
        targetByStockCode.put(stockOne, new MatchingSlotTarget(ADDRESS_ONE, true));
        sender = newSender(addressOne, addressTwo, 100);
        sender.start();
        assertThat(sender.rejectedReplyCount()).isZero();

        sender.acceptRejectedOrder(placeOrder(1L, stockOne));
        sender.acceptRejectedOrder(placeOrder(1L, stockOne));
        sender.acceptRejectedOrder(placeOrder(2L, stockTwo));

        assertThat(sender.rejectedReplyCount()).as("같은 주문이 두 번 거부되면 두 건으로 센다(이벤트 단위)").isEqualTo(3L);
    }

    @Test
    void 거부_회신의_첫_수신과_마지막_수신_시각과_보관_목록_최대_개수를_기록한다() throws InterruptedException {
        RecordingPublication addressOne = new RecordingPublication();
        RecordingPublication addressTwo = new RecordingPublication();
        targetByStockCode.put(stockOne, new MatchingSlotTarget(ADDRESS_ONE, true));
        sender = newSender(addressOne, addressTwo, 100);
        sender.start();
        assertThat(sender.firstRejectedReplyEpochMillis()).isZero();
        long before = System.currentTimeMillis();

        sender.acceptRejectedOrder(placeOrder(1L, stockOne));
        Thread.sleep(30);
        sender.acceptRejectedOrder(placeOrder(2L, stockOne));
        await(() -> sender.maxHeldOrderTotal() == 2, "두 주문이 보관 목록으로 옮겨지기");

        assertThat(sender.firstRejectedReplyEpochMillis()).isBetween(before, System.currentTimeMillis());
        assertThat(sender.lastRejectedReplyEpochMillis()).isGreaterThan(sender.firstRejectedReplyEpochMillis());
        assertThat(sender.maxHeldOrderTotal()).isEqualTo(2);
    }

    @Test
    void 재전송_간격은_첫_거부는_0이고_이후_두_배씩_늘다_상한에서_멈춘다() {
        long base = 20_000_000L;
        long cap = 100_000_000L;
        assertThat(AeronMatchingOrderSender.retransmitDelayNanos(1, base, cap)).isZero();
        assertThat(AeronMatchingOrderSender.retransmitDelayNanos(2, base, cap)).isEqualTo(base);
        assertThat(AeronMatchingOrderSender.retransmitDelayNanos(3, base, cap)).isEqualTo(2 * base);
        assertThat(AeronMatchingOrderSender.retransmitDelayNanos(4, base, cap)).isEqualTo(4 * base);
        assertThat(AeronMatchingOrderSender.retransmitDelayNanos(5, base, cap)).isEqualTo(cap);
        assertThat(AeronMatchingOrderSender.retransmitDelayNanos(60, base, cap)).as("큰 횟수에서도 넘치지 않고 상한").isEqualTo(cap);
    }

    @Test
    void 담당이_있는_pod가_같은_slot의_주문을_계속_거부해도_재전송은_간격을_두고_나간다() {
        RecordingPublication addressOne = new RecordingPublication();
        RecordingPublication addressTwo = new RecordingPublication();
        targetByStockCode.put(stockOne, new MatchingSlotTarget(ADDRESS_ONE, false));
        sender = newSender(addressOne, addressTwo, 100);
        sender.setRetransmitBackoff(500L, 4_000L, 10_000L);
        sender.start();

        long[] delayMillis = new long[3];
        for (int round = 0; round < 3; round++) {
            long acceptedAt = System.nanoTime();
            sender.acceptRejectedOrder(placeOrder(1L, stockOne));
            int expectedSends = round + 1;
            await(() -> addressOne.sent.size() == expectedSends, "재전송 " + expectedSends + "번째가 나가기");
            delayMillis[round] = (System.nanoTime() - acceptedAt) / 1_000_000L;
        }

        // 기본 간격을 500ms로 키워 두고, 첫 거부는 간격의 절반 안에, 이후는 간격의 96% 이상에 나가는지 본다(스케줄링 지연에 흔들리지 않는 여유).
        assertThat(delayMillis[0]).as("첫 거부는 바로 재전송(기본 간격의 절반 미만)").isLessThan(250L);
        assertThat(delayMillis[1]).as("연속 두 번째는 기본 간격 500ms 이상").isGreaterThanOrEqualTo(480L);
        assertThat(delayMillis[2]).as("연속 세 번째는 두 배 1,000ms 이상").isGreaterThanOrEqualTo(960L);
    }

    @Test
    void 보관한_새_주문을_이유별로_센다_이동_중_표시_담당_없음_재전송_대기_뒤() throws InterruptedException {
        RecordingPublication addressOne = new RecordingPublication();
        RecordingPublication addressTwo = new RecordingPublication();
        targetByStockCode.put(stockOne, new MatchingSlotTarget(ADDRESS_ONE, true));
        // stockTwo는 담당이 없다. stockThree는 담당이 있고 이동 중이 아니다.
        targetByStockCode.put(stockThree, new MatchingSlotTarget(ADDRESS_ONE, false));
        sender = newSender(addressOne, addressTwo, 100);
        sender.setRetransmitBackoff(300L, 1_000L, 10_000L);
        sender.start();
        assertThat(sender.heldForMovingMarkCount()).isZero();
        assertThat(sender.heldForNoOwnerCount()).isZero();
        assertThat(sender.heldBehindRetransmitCount()).isZero();

        forward(10L, stockOne);
        forward(11L, stockOne);
        forward(12L, stockTwo);
        await(() -> holdingCount(stockOne) == 2 && holdingCount(stockTwo) == 1, "새 주문 셋이 보관되기");
        assertThat(sender.heldForMovingMarkCount()).as("이동 중 표시 때문에 보관한 새 주문").isEqualTo(2);
        assertThat(sender.heldForNoOwnerCount()).as("담당이 없어 보관한 새 주문").isEqualTo(1);
        assertThat(sender.heldBehindRetransmitCount()).isZero();

        sender.acceptRejectedOrder(placeOrder(1L, stockThree));
        await(() -> addressOne.sent.size() == 1, "첫 재전송은 바로 나간다");
        sender.acceptRejectedOrder(placeOrder(2L, stockThree));
        Thread.sleep(50);
        forward(13L, stockThree);
        await(() -> holdingCount(stockThree) == 2, "재전송 대기 주문과 그 뒤 새 주문이 보관되기");
        assertThat(sender.heldBehindRetransmitCount()).as("재전송 대기 주문이 있어 뒤에 보관한 새 주문").isEqualTo(1);
        assertThat(sender.heldForMovingMarkCount()).as("거부 회신 주문은 이동 중 표시 보관에 세지 않는다").isEqualTo(2);
        assertThat(sender.heldForNoOwnerCount()).isEqualTo(1);
    }

    @Test
    void 재전송_대기_중인_slot에_새_주문이_들어와도_재전송이_먼저_나간다() throws InterruptedException {
        RecordingPublication addressOne = new RecordingPublication();
        RecordingPublication addressTwo = new RecordingPublication();
        targetByStockCode.put(stockOne, new MatchingSlotTarget(ADDRESS_ONE, false));
        sender = newSender(addressOne, addressTwo, 100);
        sender.setRetransmitBackoff(300L, 1_000L, 10_000L);
        sender.start();

        sender.acceptRejectedOrder(placeOrder(1L, stockOne));
        await(() -> addressOne.sent.size() == 1, "첫 재전송");
        sender.acceptRejectedOrder(placeOrder(2L, stockOne));
        Thread.sleep(50);
        sender.forwardPlace(3L, ACCOUNT_ID, stockOne, OrderSide.BUY, new BigDecimal("10000"), 1);

        await(() -> addressOne.sent.size() == 3, "대기가 끝난 뒤 두 건이 나가기");
        assertThat(addressOne.sentOrderIds()).as("재전송 주문(2)이 새 주문(3)보다 먼저").containsExactly(1L, 2L, 3L);
    }

    @Test
    void 보관을_비운_직후_늦게_도착한_거부_회신도_새_연속_회차로_세어_기본_간격을_둔다() throws InterruptedException {
        RecordingPublication addressOne = new RecordingPublication();
        RecordingPublication addressTwo = new RecordingPublication();
        // 맵은 이미 담당(이동 중 아님)을 가리킨다. 회신을 어느 pod가 보냈는지는 알 수 없어 늦게 온 회신과 담당이 거부한 회신을 구분하지 못한다.
        targetByStockCode.put(stockOne, new MatchingSlotTarget(ADDRESS_ONE, false));
        sender = newSender(addressOne, addressTwo, 100);
        sender.setRetransmitBackoff(500L, 4_000L, 10_000L);
        sender.start();

        sender.acceptRejectedOrder(placeOrder(1L, stockOne));
        await(() -> addressOne.sent.size() == 1, "첫 거부는 바로 나간다(보관 목록이 비워졌다)");

        long acceptedAt = System.nanoTime();
        sender.acceptRejectedOrder(placeOrder(2L, stockOne));
        Thread.sleep(150);
        assertThat(addressOne.sent).as("비운 직후 온 두 번째 회신은 새 회차(2번째)로 세어져 기본 간격 동안 기다린다").hasSize(1);
        await(() -> addressOne.sent.size() == 2, "기본 간격이 지난 뒤 나간다");
        assertThat((System.nanoTime() - acceptedAt) / 1_000_000L).isGreaterThanOrEqualTo(480L);
    }

    @Test
    void 거부가_한동안_없으면_횟수가_되돌아가_다시_바로_재전송한다() throws InterruptedException {
        RecordingPublication addressOne = new RecordingPublication();
        RecordingPublication addressTwo = new RecordingPublication();
        targetByStockCode.put(stockOne, new MatchingSlotTarget(ADDRESS_ONE, false));
        sender = newSender(addressOne, addressTwo, 100);
        sender.setRetransmitBackoff(600L, 2_000L, 150L);
        sender.start();

        sender.acceptRejectedOrder(placeOrder(1L, stockOne));
        await(() -> addressOne.sent.size() == 1, "첫 재전송");
        sender.acceptRejectedOrder(placeOrder(1L, stockOne));
        await(() -> addressOne.sent.size() == 2, "두 번째 재전송(600ms 간격)");
        Thread.sleep(400);

        long acceptedAt = System.nanoTime();
        sender.acceptRejectedOrder(placeOrder(1L, stockOne));
        await(() -> addressOne.sent.size() == 3, "되돌아간 뒤 재전송");
        assertThat((System.nanoTime() - acceptedAt) / 1_000_000L).as("조용했던 뒤의 거부는 다시 바로 재전송(기본 간격 600ms의 절반 미만)").isLessThan(300L);
    }

    @Test
    void 맵이_이미_바뀐_뒤_도착한_거부_회신은_보관하지_않고_바로_보낸다() throws InterruptedException {
        RecordingPublication addressOne = new RecordingPublication();
        RecordingPublication addressTwo = new RecordingPublication();
        targetByStockCode.put(stockOne, new MatchingSlotTarget(ADDRESS_TWO, false));
        sender = newSender(addressOne, addressTwo, 100);
        sender.start();

        sender.acceptRejectedOrder(placeOrder(5L, stockOne));

        await(() -> addressTwo.sent.size() == 1, "늦게 온 거부 회신이 바로 나가기");
        Thread.sleep(50);
        assertThat(holdingCount(stockOne)).isZero();
    }

    @Test
    void 보관_목록에_여유가_없으면_hasHoldingRoom이_false이고_쌓인_주문은_그대로이며_다른_종목은_영향이_없다() {
        RecordingPublication addressOne = new RecordingPublication();
        RecordingPublication addressTwo = new RecordingPublication();
        targetByStockCode.put(stockTwo, new MatchingSlotTarget(ADDRESS_TWO, false));
        sender = newSender(addressOne, addressTwo, 2);
        sender.start();
        assertThat(sender.hasHoldingRoom(stockOne)).isTrue();

        forward(1L, stockOne);
        forward(2L, stockOne);

        await(() -> !sender.hasHoldingRoom(stockOne), "보관 목록이 가득 차기");
        assertThat(holdingCount(stockOne)).isEqualTo(2);
        assertThat(sender.hasHoldingRoom(stockTwo)).as("다른 종목 slot은 영향이 없다").isTrue();
        assertThat(sender.hasHoldingRoom(stockThree)).isTrue();

        targetByStockCode.put(stockOne, new MatchingSlotTarget(ADDRESS_ONE, false));
        await(() -> addressOne.sent.size() == 2, "쌓인 두 주문이 그대로 나가기");
        assertThat(addressOne.sentOrderIds()).containsExactly(1L, 2L);
        assertThat(sender.hasHoldingRoom(stockOne)).isTrue();
    }

    @Test
    void offer가_BACK_PRESSURED로_재시도되는_동안에도_거부_회신_queue를_비운다() throws InterruptedException {
        RecordingPublication addressOne = new RecordingPublication();
        RecordingPublication addressTwo = new RecordingPublication();
        addressOne.offerResult.set(Publication.BACK_PRESSURED);
        targetByStockCode.put(stockOne, new MatchingSlotTarget(ADDRESS_ONE, false));
        targetByStockCode.put(stockTwo, new MatchingSlotTarget(ADDRESS_TWO, true));
        sender = newSender(addressOne, addressTwo, 100);
        sender.start();

        forward(1L, stockOne);
        Thread.sleep(100);
        sender.acceptRejectedOrder(placeOrder(2L, stockTwo));

        await(() -> sender.rejectedInboxSize() == 0 && holdingCount(stockTwo) == 1,
            "발신이 재시도로 묶인 채로 거부 회신 queue가 비워져 보관 목록으로 옮겨지기");
        assertThat(addressOne.sent).as("첫 주문은 아직 막혀 있다").isEmpty();

        addressOne.offerResult.set(100L);
        await(() -> addressOne.sent.size() == 1, "막힘이 풀린 뒤 첫 주문이 나가기");
    }

    @Test
    void 주소_풀에_없는_주소를_가리키면_발신이_멈추고_forwardPlace가_예외를_던진다() {
        RecordingPublication addressOne = new RecordingPublication();
        RecordingPublication addressTwo = new RecordingPublication();
        targetByStockCode.put(stockOne, new MatchingSlotTarget("aeron:udp?endpoint=localhost:29999", false));
        sender = newSender(addressOne, addressTwo, 100);
        sender.start();

        forward(1L, stockOne);

        await(() -> throwsOnForward(2L, stockTwo), "설정 어긋남이 forwardPlace 예외로 전파되기");
    }

    @Test
    void 조회_대역이_설정_어긋남_예외를_던져도_발신이_멈추고_forwardPlace가_예외를_던진다() {
        RecordingPublication addressOne = new RecordingPublication();
        RecordingPublication addressTwo = new RecordingPublication();
        MatchingTargetLookup misconfigured = () -> stockCode -> {
            throw new MatchingDestinationMisconfiguredException("풀에 없는 주소");
        };
        sender = new AeronMatchingOrderSender(misconfigured,
            Map.of(ADDRESS_ONE, addressOne.publication, ADDRESS_TWO, addressTwo.publication),
            stockSlotHasher, 100, new org.agrona.concurrent.BackoffIdleStrategy());
        sender.start();

        forward(1L, stockOne);

        await(() -> throwsOnForward(2L, stockTwo), "조회 예외가 forwardPlace 예외로 전파되기");
    }

    // ---- 헬퍼 ----

    private AeronMatchingOrderSender newSender(RecordingPublication addressOne, RecordingPublication addressTwo,
                                               int holdingCapacityPerSlot) {
        Map<String, Publication> publications = Map.of(
            ADDRESS_ONE, addressOne.publication,
            ADDRESS_TWO, addressTwo.publication);
        return new AeronMatchingOrderSender(lookupDouble, publications, stockSlotHasher, holdingCapacityPerSlot,
            new org.agrona.concurrent.BackoffIdleStrategy());
    }

    private void forward(long orderId, String stockCode) {
        sender.forwardPlace(orderId, ACCOUNT_ID, stockCode, OrderSide.BUY, new BigDecimal("10000"), 1);
    }

    private JournaledOrder placeOrder(long orderId, String stockCode) {
        return new JournaledOrder(EventType.PLACE, orderId, ACCOUNT_ID, stockCode, OrderSide.BUY,
            new BigDecimal("10000"), 1, Instant.now(), 0L, 0L, 0L);
    }

    private int holdingCount(String stockCode) {
        return sender.holdingCount(stockCode);
    }

    private boolean throwsOnForward(long orderId, String stockCode) {
        try {
            forward(orderId, stockCode);
            return false;
        } catch (IllegalStateException expected) {
            return true;
        }
    }

    private String stockInDifferentSlot(String... excludedStockCodes) {
        for (int candidate = 1; candidate < 1000; candidate++) {
            String candidateCode = String.format("%06d", candidate);
            boolean sharesSlot = false;
            for (String excluded : excludedStockCodes) {
                if (stockSlotHasher.slotFor(candidateCode) == stockSlotHasher.slotFor(excluded)) {
                    sharesSlot = true;
                }
            }
            if (!sharesSlot) {
                return candidateCode;
            }
        }
        throw new IllegalStateException("다른 종목 slot의 종목코드를 찾지 못했다");
    }

    private void await(BooleanSupplier condition, String description) {
        long deadline = System.nanoTime() + AWAIT_MILLIS * 1_000_000L;
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new AssertionError(AWAIT_MILLIS + "ms 안에 이루어지지 않음: " + description);
    }
}
