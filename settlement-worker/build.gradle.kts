// v2 정산 워커 호스트 앱. account-worker/matching-worker와 달리 DB를 가진다(정산=돈·내구성 경계).
// settlement-requests를 소비해 미수금을 자체 DB에 저장한다(a2-2). T+2 스캔·되돌림 발행은 a2-3.
plugins {
	`java-convention`
	alias(libs.plugins.spring.boot)
}

dependencies {
	implementation(project(":core")) // SettlementRequestEvent, KafkaTopics

	implementation(libs.spring.boot.starter.data.jpa)
	implementation(libs.spring.kafka)
	implementation("com.fasterxml.jackson.core:jackson-databind") // Spring Kafka JsonSerializer/Deserializer 런타임 필요(버전은 Spring Boot BOM)

	runtimeOnly(libs.postgresql)
	runtimeOnly("com.h2database:h2")

	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)

	testImplementation(libs.spring.boot.starter.test)
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
