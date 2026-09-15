// v2 매칭 호스트 앱. matching-disruptor(프레임워크 0 엔진 라이브러리)를 감싸 실행·Kafka 배선을 담당한다.
// ADR-018과 같은 결(account-worker 대칭): 코어는 Spring 없는 라이브러리로 유지, 실행 진입점·Kafka는 이 별도 호스트 모듈에 둔다.
plugins {
	`java-convention`
	alias(libs.plugins.spring.boot)
}

dependencies {
	implementation(project(":core"))              // TradeFilledEvent, KafkaTopics, SnowflakeIdGenerator
	implementation(project(":trading"))            // FillResult
	implementation(project(":matching-disruptor"))  // MatchingEngine(순수 라이브러리)
	implementation(libs.disruptor)                 // MatchingEngine 생성자 타입(WaitStrategy 등)

	// Aeron: 주문 인테이크 IPC 수신 배선(파이프라인 연결 ①, account-worker C5-1b 미러). MediaDriver·
	// Aeron·Subscription 타입이 이 모듈 코드(MatchingOrderIntakeConfig)에 직접 등장해 implementation
	// 재선언이 필요하다(matching-disruptor의 implementation 의존은 여기로 전이되지 않는다).
	implementation(libs.aeron.driver)
	implementation(libs.aeron.client)
	implementation(libs.aeron.archive) // 매칭 저널 durable 녹화(Archive, 2c-1)

	implementation(libs.spring.boot.starter)
	implementation(libs.spring.kafka)
	implementation("com.fasterxml.jackson.core:jackson-databind") // Spring Kafka JsonSerializer 런타임 필요(버전은 Spring Boot BOM)

	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)

	testImplementation(libs.spring.boot.starter.test)
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// Aeron/Agrona는 JDK 17에서 내부 클래스 jdk.internal.misc.Unsafe와 sun.nio.ch에 접근한다.
// 모듈 시스템이 기본으로 막으므로 테스트 JVM에 개방 플래그를 준다(account-worker와 동일 이유).
tasks.withType<Test>().configureEach {
	jvmArgs(
		"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
		"--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED"
	)
}

// fork1, Unit 3a — bootRun은 별도 JVM으로 뜨는 실행 태스크라 위 test 개방 플래그가 안 미러된다.
// 3-JVM 로컬 실행(gradle bootRun)도 Archive/MediaDriver를 띄우므로 같은 플래그가 필요하다.
tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
	jvmArgs(
		"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
		"--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED"
	)
}
