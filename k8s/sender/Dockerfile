# 발신기(U10Tool) 이미지. U10Tool은 account-worker 테스트 소스에 있어 bootJar에 들어 있지 않다.
# 그래서 account-worker 이미지를 재사용하지 않고, u10Tool gradle 작업과 같은 클래스패스(test runtimeClasspath)를 폴더로 모아 넣는다.
# 준비(워크트리 루트에서): ./gradlew --offline -q -I k8s/sender/u10tool-classpath.init.gradle.kts :account-worker:exportU10ToolClasspath
# 빌드: docker build -t stock-trading-engine/u10-sender:local -f k8s/sender/Dockerfile account-worker/build/u10tool
# 사용: docker run --rm stock-trading-engine/u10-sender:local codes-many 64 32 3
FROM eclipse-temurin:17-jre@sha256:2a996a534890f1bee252fab23a0c01e773e9e1dc0e4700e8f0f6a2697c6cbf76
COPY lib /app/lib
COPY classes /app/classes
ENTRYPOINT ["java", "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED", "-cp", "/app/classes:/app/lib/*", "com.flab.stocktradingengine.account.worker.u10.U10Tool"]
