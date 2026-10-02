import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * k6 단독 포화점 리허설(measure-protocol-c.md U7b)용 더미 주문 서버. 실제 api와 같은 경로
 * (POST /api/v1|v2/orders/buy|sell)에 요청 본문을 읽고 202를 즉시 돌려준다. 계좌 조회, 검증, 저장이 없다.
 * 서버가 병목이 되지 않도록 요청마다 가상 스레드로 처리하고 상태는 LongAdder 와 원자 변수로만 갱신한다.
 *
 * 보조 경로: GET /__health(200), GET /__stats({count, firstNanos, lastNanos}), POST /__reset(카운터 초기화).
 * 실행: java loadtest/DummyOrderServer.java <포트>   (JDK 21 소스 파일 실행)
 */
public final class DummyOrderServer {

    private static final byte[] ACCEPTED = "{\"status\":\"ACCEPTED\"}".getBytes(StandardCharsets.UTF_8);
    private static final LongAdder COUNT = new LongAdder();
    private static final AtomicLong FIRST_NANOS = new AtomicLong(0);
    private static final AtomicLong LAST_NANOS = new AtomicLong(0);

    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 9790;
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 4096);
        server.createContext("/api/", DummyOrderServer::handleOrder);
        server.createContext("/__health", exchange -> reply(exchange, 200, "ok".getBytes(StandardCharsets.UTF_8)));
        server.createContext("/__stats", DummyOrderServer::handleStats);
        server.createContext("/__reset", DummyOrderServer::handleReset);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        System.out.println("READY " + port);
    }

    private static void handleOrder(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        boolean orderPath = "POST".equals(exchange.getRequestMethod()) && path.matches("/api/v[12]/orders/(buy|sell)");
        drain(exchange.getRequestBody());
        if (!orderPath) {
            reply(exchange, 404, new byte[0]);
            return;
        }
        long now = System.nanoTime();
        FIRST_NANOS.compareAndSet(0, now);
        LAST_NANOS.accumulateAndGet(now, Math::max);
        COUNT.increment();
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        reply(exchange, 202, ACCEPTED);
    }

    private static void handleStats(HttpExchange exchange) throws IOException {
        String json = "{\"count\":" + COUNT.sum() + ",\"firstNanos\":" + FIRST_NANOS.get() + ",\"lastNanos\":" + LAST_NANOS.get() + "}";
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        reply(exchange, 200, json.getBytes(StandardCharsets.UTF_8));
    }

    private static void handleReset(HttpExchange exchange) throws IOException {
        drain(exchange.getRequestBody());
        COUNT.reset();
        FIRST_NANOS.set(0);
        LAST_NANOS.set(0);
        reply(exchange, 200, "reset".getBytes(StandardCharsets.UTF_8));
    }

    private static void drain(InputStream body) throws IOException {
        byte[] buffer = new byte[4096];
        while (body.read(buffer) >= 0) {
            // 본문을 끝까지 읽어야 keep-alive 연결을 다음 요청에 쓴다
        }
    }

    private static void reply(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        }
        exchange.close();
    }
}
