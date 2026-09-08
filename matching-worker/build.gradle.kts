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

	implementation(libs.spring.boot.starter)
	implementation(libs.spring.kafka)
	implementation("com.fasterxml.jackson.core:jackson-databind") // Spring Kafka JsonSerializer 런타임 필요(버전은 Spring Boot BOM)

	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)

	testImplementation(libs.spring.boot.starter.test)
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
