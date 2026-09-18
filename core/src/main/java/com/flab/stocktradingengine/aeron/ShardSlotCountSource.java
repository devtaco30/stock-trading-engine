package com.flab.stocktradingengine.aeron;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/**
 * 칸 개수를 조정 토픽의 파티션 개수에서 읽어 온다.
 *
 * <p>칸 개수는 api·매칭 워커·계좌 워커가 <b>모두 같은 값</b>을 써야 한다. 하나라도 다르면 보내는
 * 앱과 받는 워커가 서로 다른 칸 번호를 계산해, 주문이 도착해도 받은 워커가 자기 담당이 아니라며
 * 거부한다. 앱마다 설정 파일에 같은 숫자를 사람이 적어 맞추는 방식은 한 곳만 틀려도 그렇게 된다.</p>
 *
 * <p>그래서 조정 모드에서는 이미 한 곳에 있는 값을 읽는다 — 조정 토픽
 * {@link #ASSIGNMENT_TOPIC}의 파티션 개수가 곧 칸 개수다(파티션 번호를 칸 번호로 쓰는 구조라
 * 정의상 같다). Kafka 프로듀서가 파티션 개수를 설정에 적지 않고 브로커 메타데이터에서 읽는 것과
 * 같은 방식이다.</p>
 *
 * <p><b>이 값은 늘릴 수 없다.</b> 파티션을 늘리면 나누는 수가 바뀌어 거의 모든 계좌의 칸이
 * 달라지고, 계좌 소유가 통째로 뒤집힌다.</p>
 */
public final class ShardSlotCountSource {

    /** 워커들이 칸을 나눠 갖는 데 쓰는 토픽. 메시지는 안 보내고 파티션 번호만 쓴다. */
    public static final String ASSIGNMENT_TOPIC = "account-shard-assignment";

    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(1);

    private ShardSlotCountSource() {
    }

    /**
     * 조정 토픽의 파티션 개수를 읽는다. 토픽이 아직 없으면 생길 때까지 기다린다 — 이 토픽은
     * 계좌 워커가 기동할 때 만들므로, api나 매칭 워커가 먼저 뜨면 잠깐 없을 수 있다.
     *
     * @throws IllegalStateException 제한 시간 안에 토픽이 안 생기면 던진다. 기동을 멈추는 편이,
     *                               칸 개수를 모르는 채로 떠서 모든 주문을 틀린 워커로 보내는 것보다 낫다.
     */
    public static int readSlotCount(Admin admin, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            try {
                Map<String, TopicDescription> described =
                    admin.describeTopics(List.of(ASSIGNMENT_TOPIC)).allTopicNames().get();
                return described.get(ASSIGNMENT_TOPIC).partitions().size();
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                    throw new IllegalStateException("조정 토픽 조회 실패: " + ASSIGNMENT_TOPIC, e);
                }
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException(
                        "조정 토픽 " + ASSIGNMENT_TOPIC + "이 " + timeout.toSeconds()
                            + "초 안에 생기지 않았습니다 — 계좌 워커를 먼저 띄워야 합니다");
                }
                sleep();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("조정 토픽 조회 중 인터럽트됨", e);
            }
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(RETRY_INTERVAL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("조정 토픽을 기다리는 중 인터럽트됨", e);
        }
    }
}
