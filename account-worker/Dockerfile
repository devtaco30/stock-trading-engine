# 계좌 서버(account-worker) 이미지. jar는 미리 빌드한다: ./gradlew --offline -q :account-worker:bootJar
# 빌드(워크트리 루트에서): docker build -t stock-trading-engine/account-worker:local account-worker
# 힙·GC 로그 같은 JVM 옵션은 매니페스트의 JDK_JAVA_OPTIONS 환경 변수로 넘긴다(java 실행기가 읽는다).
# --add-opens 두 개는 loadtest/s0b/mix.sh의 JO와 같다. Aeron/Agrona가 JDK 17 내부 클래스에 접근한다.
FROM eclipse-temurin:17-jre@sha256:2a996a534890f1bee252fab23a0c01e773e9e1dc0e4700e8f0f6a2697c6cbf76
ARG JAR=build/libs/account-worker-0.0.1-SNAPSHOT.jar
COPY ${JAR} /app/app.jar
ENTRYPOINT ["java", "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED", "-jar", "/app/app.jar"]
