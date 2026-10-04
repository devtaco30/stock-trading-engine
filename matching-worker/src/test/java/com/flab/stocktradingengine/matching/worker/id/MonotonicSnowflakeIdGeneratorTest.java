package com.flab.stocktradingengine.matching.worker.id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.relops.snowflake.Snowflake;

/**
 * 시계가 뒤로 가도 예외 없이 번호가 엄격히 늘어나는지(LLD_F 부록 A4의 1~4, 6번 항목)를 확인한다.
 * 번호 배치는 (시각 &lt;&lt; 22) | (node &lt;&lt; 12) | 순번 이다.
 */
class MonotonicSnowflakeIdGeneratorTest {

    private static final int NODE = 2;
    private static final int SEQ_BITS = 12;
    private static final int TIME_SHIFT = 22;
    private static final long SEQ_MASK = (1L << SEQ_BITS) - 1;

    /** 테스트가 값을 직접 정하는 시계. */
    private static final class FakeClock implements LongSupplier {
        private volatile long millis;

        FakeClock(long millis) {
            this.millis = millis;
        }

        void set(long millis) {
            this.millis = millis;
        }

        @Override
        public long getAsLong() {
            return millis;
        }
    }

    private static long layout(long time, long node, long seq) {
        return (time << TIME_SHIFT) | (node << SEQ_BITS) | seq;
    }

    @Test
    void 순방향_시계에서_실제_라이브러리와_같은_번호를_낸다() throws InterruptedException {
        // relops Snowflake는 시스템 시계를 직접 읽으므로, 라이브러리가 낸 번호에서 시각을 읽어
        // 그 시각을 그대로 새 발급기의 시계로 재생해 100만 개의 번호가 일치하는지 비교한다.
        Snowflake library = new Snowflake(NODE);
        int count = 1_000_000;
        long[] expected = new long[count];
        for (int i = 0; i < count; i++) {
            expected[i] = library.next();
            if (i % 1000 == 999) {
                Thread.sleep(1); // 한 ms 안에 4096개를 넘겨 라이브러리가 예외를 내지 않게 한다
            }
        }

        AtomicLong cursor = new AtomicLong();
        MonotonicSnowflakeIdGenerator generator = new MonotonicSnowflakeIdGenerator(
            NODE, () -> expected[(int) cursor.get()] >>> TIME_SHIFT);

        for (int i = 0; i < count; i++) {
            cursor.set(i);
            assertThat(generator.nextId()).isEqualTo(expected[i]);
        }
    }

    @Test
    void 시계가_뒤로_가도_예외_없이_엄격히_증가한다() {
        long[] regressionsMillis = {136L, 1_833L, 30_000L};
        for (long back : regressionsMillis) {
            FakeClock clock = new FakeClock(1_791_090_666_529L);
            MonotonicSnowflakeIdGenerator generator = new MonotonicSnowflakeIdGenerator(NODE, clock);
            long before = generator.nextId();

            clock.set(1_791_090_666_529L - back);
            List<Long> ids = new ArrayList<>();
            assertThatCode(() -> {
                for (int i = 0; i < 100; i++) {
                    ids.add(generator.nextId());
                }
            }).as("시계 %dms 역행", back).doesNotThrowAnyException();

            long previous = before;
            for (long id : ids) {
                assertThat(id).as("시계 %dms 역행", back).isGreaterThan(previous);
                previous = id;
            }
        }
    }

    @Test
    void 시계가_따라잡으면_새_시각과_순번_0으로_돌아온다() {
        FakeClock clock = new FakeClock(1_000L);
        MonotonicSnowflakeIdGenerator generator = new MonotonicSnowflakeIdGenerator(NODE, clock);
        generator.nextId();
        clock.set(500L);
        generator.nextId();

        clock.set(1_001L);

        assertThat(generator.nextId()).isEqualTo(layout(1_001L, NODE, 0));
    }

    @Test
    void 같은_ms의_4097번째_번호는_다음_ms의_순번_0이다() {
        FakeClock clock = new FakeClock(5_000L);
        MonotonicSnowflakeIdGenerator generator = new MonotonicSnowflakeIdGenerator(NODE, clock);

        long last = 0;
        for (int i = 0; i < 4096; i++) {
            last = generator.nextId();
        }
        long overflow = generator.nextId();

        assertThat(last).isEqualTo(layout(5_000L, NODE, 4095));
        assertThat(overflow).isEqualTo(layout(5_001L, NODE, 0));
    }

    @Test
    void 순번이_넘쳐_앞선_시각을_쓴_뒤에도_시계가_그_시각을_지나면_다시_시계를_따른다() {
        FakeClock clock = new FakeClock(5_000L);
        MonotonicSnowflakeIdGenerator generator = new MonotonicSnowflakeIdGenerator(NODE, clock);
        for (int i = 0; i < 4097; i++) {
            generator.nextId(); // 마지막 번호는 시각 5,001, 순번 0
        }

        clock.set(5_002L);

        assertThat(generator.nextId()).isEqualTo(layout(5_002L, NODE, 0));
    }

    @Test
    void 임의로_움직이는_시계로_100만_번_호출해도_번호가_겹치지_않는다() {
        Random random = new Random(42);
        FakeClock clock = new FakeClock(1_000_000L);
        MonotonicSnowflakeIdGenerator generator = new MonotonicSnowflakeIdGenerator(NODE, clock);
        Set<Long> seen = new HashSet<>(2_000_000);
        long previous = Long.MIN_VALUE;

        for (int i = 0; i < 1_000_000; i++) {
            int step = random.nextInt(10);
            if (step == 0) {
                clock.set(clock.getAsLong() - random.nextInt(2_000)); // 뒤로
            } else if (step < 3) {
                clock.set(clock.getAsLong() + random.nextInt(3)); // 앞으로 조금
            }
            long id = generator.nextId();
            assertThat(id).isGreaterThan(previous);
            assertThat(seen.add(id)).as("중복 번호 %d", id).isTrue();
            previous = id;
        }
    }

    @Test
    void 여덟_스레드가_동시에_호출해도_번호가_겹치지_않는다() throws Exception {
        FakeClock clock = new FakeClock(2_000_000L);
        MonotonicSnowflakeIdGenerator generator = new MonotonicSnowflakeIdGenerator(NODE, clock);
        int threads = 8;
        int perThread = 100_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<java.util.concurrent.Future<long[]>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            futures.add(pool.submit(() -> {
                long[] ids = new long[perThread];
                start.await();
                for (int i = 0; i < perThread; i++) {
                    if (i % 5000 == 0) {
                        clock.set(clock.getAsLong() - 3); // 동시 호출 중에도 시계가 뒤로 간다
                    }
                    ids[i] = generator.nextId();
                }
                return ids;
            }));
        }
        start.countDown();
        Set<Long> all = new HashSet<>(threads * perThread * 2);
        for (java.util.concurrent.Future<long[]> future : futures) {
            for (long id : future.get()) {
                assertThat(all.add(id)).as("중복 번호 %d", id).isTrue();
            }
        }
        pool.shutdown();

        assertThat(all).hasSize(threads * perThread);
    }

    @Test
    void 노드_번호는_번호의_노드_칸에_들어가고_범위_밖은_클램프한다() {
        FakeClock clock = new FakeClock(7L);

        assertThat(new MonotonicSnowflakeIdGenerator(1023, clock).nextId()).isEqualTo(layout(7L, 1023, 0));
        assertThat(new MonotonicSnowflakeIdGenerator(5000, clock).nextId()).isEqualTo(layout(7L, 1023, 0));
        assertThat(new MonotonicSnowflakeIdGenerator(-3, clock).nextId()).isEqualTo(layout(7L, 0, 0));
    }

    @Test
    void 기본_생성자는_시스템_시계를_쓴다() {
        MonotonicSnowflakeIdGenerator generator = new MonotonicSnowflakeIdGenerator(NODE);

        long id = generator.nextId();
        long now = System.currentTimeMillis();

        assertThat(id >>> TIME_SHIFT).isBetween(now - 1_000L, now + 1_000L);
        assertThat((id >> SEQ_BITS) & 1023).isEqualTo(NODE);
        assertThat(id & SEQ_MASK).isZero();
    }

    /** LLD_F 부록 A4의 여섯째 항목: 번호에서 시각을 읽는 코드가 새로 생기지 않았는지 소스를 검사한다. */
    @Test
    void v2_소스에_번호에서_시각을_읽는_코드가_없다() throws IOException {
        Pattern decode = Pattern.compile(">>>?\\s*(22|\\(?\\s*(NODE|SEQ)_SHIFT)");
        Path repoRoot = Path.of("..").toAbsolutePath().normalize();
        List<String> modules = List.of(
            "matching-worker", "matching-disruptor", "account-worker", "account-disruptor");
        List<String> offenders = new ArrayList<>();

        for (String module : modules) {
            Path sourceRoot = repoRoot.resolve(module).resolve("src/main/java");
            if (!Files.isDirectory(sourceRoot)) {
                continue;
            }
            try (Stream<Path> files = Files.walk(sourceRoot)) {
                for (Path file : (Iterable<Path>) files.filter(p -> p.toString().endsWith(".java"))::iterator) {
                    if (file.getFileName().toString().equals("MonotonicSnowflakeIdGenerator.java")) {
                        continue;
                    }
                    List<String> lines = Files.readAllLines(file);
                    for (int i = 0; i < lines.size(); i++) {
                        if (decode.matcher(lines.get(i)).find()) {
                            offenders.add(repoRoot.relativize(file) + ":" + (i + 1) + " " + lines.get(i).trim());
                        }
                    }
                }
            }
        }

        assertThat(offenders).as("번호에서 시각을 읽는 코드").isEmpty();
    }
}
