package com.flab.stocktradingengine.support;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * SnowflakeIdGenerator 동작 검증.
 * <p>ID가 어떻게 나오는지(형식·유일성·시간순·동시성) 확인.</p>
 */
@DisplayName("SnowflakeIdGenerator 테스트")
class SnowflakeIdGeneratorTest {

    @Nested
    @DisplayName("ID 값 형식·유일성")
    class IdFormatAndUniqueness {

        @Test
        @DisplayName("nextId()는 양의 64비트 long을 반환한다")
        void nextId_returnsPositiveLong() {
            SnowflakeIdGenerator generator = new SnowflakeIdGenerator(0L);

            long id = generator.nextId();

            System.out.println("id: " + id);
            assertTrue(id > 0, "id must be positive");
            assertTrue(id <= Long.MAX_VALUE, "id must be within long range");
        }

        @Test
        @DisplayName("연속 호출 시 매번 다른 ID가 나온다")
        void nextId_returnsUniqueIds() {
            SnowflakeIdGenerator generator = new SnowflakeIdGenerator(0L);
            Set<Long> ids = new HashSet<>();

            for (int i = 0; i < 500; i++) {
                long id = generator.nextId();
                System.out.println(i + "번째 id: " + id);
                assertTrue(ids.add(id), "duplicate id: " + id);
            }
        }

        @Test
        @DisplayName("노드가 다르면 같은 시점에도 다른 ID가 나온다")
        void differentNodes_produceDifferentIds() {
            SnowflakeIdGenerator node0 = new SnowflakeIdGenerator(0L);
            SnowflakeIdGenerator node1 = new SnowflakeIdGenerator(1L);

            long id0 = node0.nextId();
            long id1 = node1.nextId();

            System.out.println("node0 id: " + id0);
            System.out.println("node1 id: " + id1);
            assertNotEquals(id0, id1);
        }
    }

    @Nested
    @DisplayName("시간순 정렬")
    class TimeOrdering {

        @Test
        @DisplayName("나중에 호출한 ID가 이전 ID보다 크다 (시간순)")
        void laterCall_returnsGreaterId() {
            SnowflakeIdGenerator generator = new SnowflakeIdGenerator(0L);

            long first = generator.nextId();
            long second = generator.nextId();

            System.out.println("first: " + first);
            System.out.println("second: " + second);
            assertTrue(second > first, "ids should be time-ordered");
        }
    }

    @Nested
    @DisplayName("노드 ID 클램프")
    class NodeIdClamp {

        @Test
        @DisplayName("nodeId가 0 미만이면 0으로 클램프되어 동작한다")
        void negativeNodeId_clampedToZero() {
            SnowflakeIdGenerator generator = new SnowflakeIdGenerator(-1L);

            long id = assertDoesNotThrow(generator::nextId);

            System.out.println("nodeId -1 id: " + id);
            assertTrue(id > 0);
        }

        @Test
        @DisplayName("nodeId가 1023 초과면 1023으로 클램프되어 동작한다")
        void nodeIdOver1023_clampedTo1023() {
            SnowflakeIdGenerator generator = new SnowflakeIdGenerator(2000L);

            long id = assertDoesNotThrow(generator::nextId);

            System.out.println("nodeId 2000 id: " + id);
            assertTrue(id > 0);
        }
    }

    @Nested
    @DisplayName("동시 호출")
    class ConcurrentCalls {

        @Test
        @DisplayName("여러 스레드에서 동시에 nextId() 호출해도 유일한 ID만 나온다")
        void concurrentNextId_allUnique() throws InterruptedException {
            // given
            SnowflakeIdGenerator generator = new SnowflakeIdGenerator(0L);
            int threadCount = 10;
            int callsPerThread = 100;

            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            // start: countDown 1번이 되면 await 중인 스레드 전원이 동시에 풀림 → 10개 스레드 동시 시작용
            CountDownLatch start = new CountDownLatch(1);
            // done: 각 스레드가 끝날 때마다 countDown 한 번씩. 메인은 await()로 10개가 다 끝날 때까지 대기
            CountDownLatch done = new CountDownLatch(threadCount);

            Set<Long> ids = new HashSet<>();
            Object lock = new Object();

            for (int t = 0; t < threadCount; t++) {
                // 각 스레드가 할 task
                executor.submit(() -> {
                    try {
                        // start latch를 보고 대기하다가, 메인에서 countDown()하면 풀림 → 그때부터 작업 시작
                        start.await();
                        for (int i = 0; i < callsPerThread; i++) {
                            long id = generator.nextId();
                            // ids는 여러 스레드가 공유하므로, add 시 동시성 이슈 방지를 위해 동기화
                            synchronized (lock) {
                                ids.add(id);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    } finally {
                        // 이 스레드가 할 일을 다 끝냈음을 알림. 10개 스레드가 각자 한 번씩 호출하면 done이 0이 됨
                        done.countDown();
                    }
                });
            }
            // start를 0으로 만들어 await 중이던 10개 스레드를 한꺼번에 풀어서 동시에 nextId() 호출 시작
            start.countDown();
            // 10개 스레드가 전부 done.countDown() 할 때까지 메인 스레드 대기 (다 끝난 뒤에 검증해야 하므로)
            done.await();
            executor.shutdown();

            System.out.println("ids size: " + ids.size());

            // 기대: 10 스레드 × 100회 = 1000개의 서로 다른 ID. 실패 시에만 아래 메시지가 찍힘
            int expectedCount = threadCount * callsPerThread;
            assertTrue(ids.size() == expectedCount,
                "expected " + expectedCount + " unique ids, got " + ids.size());
        }
    }

    @Nested
    @DisplayName("ID 값이 어떻게 나오는지 (구성 확인)")
    class HowIdIsProduced {

        @Test
        @DisplayName("같은 노드에서 연속 발급 시 ID는 단조 증가한다")
        void idsFromSameNode_areMonotonicallyIncreasing() {
            SnowflakeIdGenerator generator = new SnowflakeIdGenerator(5L);
            AtomicLong prev = new AtomicLong(0L);

            assertAll(() -> {
                for (int i = 0; i < 100; i++) {
                    long id = generator.nextId();
                    System.out.println("id: " + id);
                    assertTrue(id > prev.get(), "id should be monotonically increasing");
                    prev.set(id);
                }
            });
        }
    }
}
