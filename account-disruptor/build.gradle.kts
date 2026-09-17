// 계좌 축 워커 — 프레임워크 없는 순수 라이브러리(matching-disruptor 와 동일한 결).
// spring-boot 플러그인 없음. accountId single-writer 인메모리 검증·예약을 Disruptor 로 돌린다.
plugins {
	`java-convention`
	`java-library`
}

// JMH 벤치마크 전용 소스셋. matching-disruptor와 같은 이유로 서드파티 JMH Gradle 플러그인에
// 기대지 않고 소스셋을 직접 만들어, jmh 코드가 main 산출물과 프로젝트 의존성을 그대로 쓰게 배선한다.
val jmh: SourceSet by sourceSets.creating

// jmh 소스셋의 의존성 구성이 main의 implementation/runtimeOnly를 상속하게 한다.
configurations["jmhImplementation"].extendsFrom(configurations.implementation.get())
configurations["jmhRuntimeOnly"].extendsFrom(configurations.runtimeOnly.get())

dependencies {
	implementation(project(":core"))     // Snowflake, 공유 타입
	implementation(project(":trading"))  // BuyOrderCommand 재사용

	implementation(libs.disruptor)       // 단일 소비자 링버퍼 (하네스는 B2-2)

	// Aeron: 주문 인테이크 IPC 수신(C5-1b). matching-disruptor의 AeronOrderReceiver와 같은 이유.
	implementation(libs.aeron.driver)
	implementation(libs.aeron.client)

	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)

	testImplementation(libs.junit.jupiter)
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")

	// JMH 코어(런타임)와 벤치 클래스 생성용 애노테이션 프로세서
	"jmhImplementation"(libs.jmh.core)
	"jmhAnnotationProcessor"(libs.jmh.generator.annprocess)
}

// Aeron/Agrona는 JDK 17에서 내부 클래스 jdk.internal.misc.Unsafe와 sun.nio.ch에 접근한다.
// 모듈 시스템이 기본으로 막으므로 테스트 JVM에 개방 플래그를 준다(matching-disruptor와 동일 이유).
tasks.withType<Test>().configureEach {
	jvmArgs(
		"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
		"--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED"
	)
}

// jmh 소스셋이 main의 컴파일 산출물(AccountSnapshotCodec 등)을 참조하도록 classpath에 더한다.
sourceSets.named("jmh") {
	compileClasspath += sourceSets.main.get().output
	runtimeClasspath += sourceSets.main.get().output
}

// 실행: ./gradlew :account-disruptor:jmh --args="-wi 3 -i 5 -f 1"
tasks.register<JavaExec>("jmh") {
	group = "benchmark"
	description = "JMH 계좌 스냅샷 인코딩 벤치마크 실행"
	mainClass.set("org.openjdk.jmh.Main")
	classpath = sourceSets.named("jmh").get().runtimeClasspath
}
