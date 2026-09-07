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

	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)

	testImplementation(libs.junit.jupiter)
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
