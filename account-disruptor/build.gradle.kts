// 계좌 축 워커 — 프레임워크 없는 순수 라이브러리(matching-disruptor 와 동일한 결).
// spring-boot 플러그인 없음. accountId single-writer 인메모리 검증·예약을 Disruptor 로 돌린다.
plugins {
	`java-convention`
	`java-library`
}

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
}

// Aeron/Agrona는 JDK 17에서 내부 클래스 jdk.internal.misc.Unsafe와 sun.nio.ch에 접근한다.
// 모듈 시스템이 기본으로 막으므로 테스트 JVM에 개방 플래그를 준다(matching-disruptor와 동일 이유).
tasks.withType<Test>().configureEach {
	jvmArgs(
		"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
		"--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED"
	)
}
